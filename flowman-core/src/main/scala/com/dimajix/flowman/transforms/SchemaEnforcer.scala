/*
 * Copyright 2018-2022 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.transforms

import java.util.Locale

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.functions.rpad
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.CharType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.VarcharType

import com.dimajix.common.MapIgnoreCase
import com.dimajix.flowman.execution.SchemaMismatchException
import com.dimajix.flowman.util.SchemaUtils.coerce
import com.dimajix.spark.sql.SchemaUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY
import com.dimajix.spark.sql.functions.nullable_struct


sealed abstract class ColumnMismatchPolicy extends Product with Serializable
object ColumnMismatchPolicy {
    case object IGNORE extends ColumnMismatchPolicy
    case object ERROR extends ColumnMismatchPolicy
    case object ADD_COLUMNS_OR_IGNORE extends ColumnMismatchPolicy
    case object ADD_COLUMNS_OR_ERROR extends ColumnMismatchPolicy
    case object REMOVE_COLUMNS_OR_IGNORE extends ColumnMismatchPolicy
    case object REMOVE_COLUMNS_OR_ERROR extends ColumnMismatchPolicy
    case object ADD_REMOVE_COLUMNS extends ColumnMismatchPolicy

    def ofString(mode:String) : ColumnMismatchPolicy = {
        mode.toLowerCase(Locale.ROOT) match {
            case "ignore" => IGNORE
            case "error" => ERROR
            case "add_columns_or_ignore"|"addcolumnsorignore" => ADD_COLUMNS_OR_IGNORE
            case "add_columns_or_error"|"addcolumnsorerror" => ADD_COLUMNS_OR_ERROR
            case "remove_columns_or_ignore"|"removecolumnsorignore" => REMOVE_COLUMNS_OR_IGNORE
            case "remove_columns_or_error"|"removecolumnsorerror" => REMOVE_COLUMNS_OR_ERROR
            case "add_remove_columns"|"addremovecolumnso" => ADD_REMOVE_COLUMNS
            case _ => throw new IllegalArgumentException(s"Unknown column mismatch policy: '$mode'. " +
                "Accepted error strategies are 'ignore', 'error', 'add_columns_or_ignore', 'add_columns_or_error', 'remove_columns_or_ignore', 'remove_columns_or_error', 'add_remove_columns'.")
        }
    }
}


sealed abstract class TypeMismatchPolicy extends Product with Serializable
object TypeMismatchPolicy {
    case object IGNORE extends TypeMismatchPolicy
    case object ERROR extends TypeMismatchPolicy
    case object CAST_COMPATIBLE_OR_ERROR extends TypeMismatchPolicy
    case object CAST_COMPATIBLE_OR_IGNORE extends TypeMismatchPolicy
    case object CAST_ALWAYS extends TypeMismatchPolicy

    def ofString(mode:String) : TypeMismatchPolicy = {
        mode.toLowerCase(Locale.ROOT) match {
            case "ignore" => IGNORE
            case "error" => ERROR
            case "cast_compatible_or_error"|"castcompatibleorerror" => CAST_COMPATIBLE_OR_ERROR
            case "cast_compatible_or_ignore"|"castcompatibleorignore" => CAST_COMPATIBLE_OR_IGNORE
            case "cast_always"|"castalways" => CAST_ALWAYS
            case _ => throw new IllegalArgumentException(s"Unknown type mismatch policy: '$mode'. " +
                "Accepted error strategies are 'ignore', 'error', 'cast_compatible_or_error', 'cast_compatible_or_ignore', 'cast_always'.")
        }
    }
}

sealed abstract class CharVarcharPolicy extends Product with Serializable
object CharVarcharPolicy {
    case object PAD extends CharVarcharPolicy
    case object TRUNCATE extends CharVarcharPolicy
    case object PAD_AND_TRUNCATE extends CharVarcharPolicy
    case object IGNORE extends CharVarcharPolicy

    def ofString(mode: String): CharVarcharPolicy = {
        mode.toLowerCase(Locale.ROOT) match {
            case "ignore" => IGNORE
            case "truncate" => TRUNCATE
            case "pad" => PAD
            case "pad_and_truncate" | "padandtruncate" => PAD_AND_TRUNCATE
            case _ => throw new IllegalArgumentException(s"Unknown char/varchar policy: '$mode'. " +
                "Accepted error strategies are 'ignore', 'truncate', 'pad', 'pad_and_truncate'.")
        }
    }
}


final case class SchemaEnforcer(
    schema:StructType,
    columnMismatchPolicy:ColumnMismatchPolicy=ColumnMismatchPolicy.ADD_REMOVE_COLUMNS,
    typeMismatchPolicy:TypeMismatchPolicy=TypeMismatchPolicy.CAST_ALWAYS,
    charVarcharPolicy:CharVarcharPolicy=CharVarcharPolicy.PAD_AND_TRUNCATE
) {
    private val addColumns =
        columnMismatchPolicy == ColumnMismatchPolicy.ADD_COLUMNS_OR_IGNORE ||
            columnMismatchPolicy == ColumnMismatchPolicy.ADD_COLUMNS_OR_ERROR ||
            columnMismatchPolicy == ColumnMismatchPolicy.ADD_REMOVE_COLUMNS
    private val removeColumns =
        columnMismatchPolicy == ColumnMismatchPolicy.REMOVE_COLUMNS_OR_IGNORE ||
            columnMismatchPolicy == ColumnMismatchPolicy.REMOVE_COLUMNS_OR_ERROR ||
            columnMismatchPolicy == ColumnMismatchPolicy.ADD_REMOVE_COLUMNS
    private val ignoreColumns = columnMismatchPolicy == ColumnMismatchPolicy.IGNORE ||
        columnMismatchPolicy == ColumnMismatchPolicy.REMOVE_COLUMNS_OR_IGNORE ||
        columnMismatchPolicy == ColumnMismatchPolicy.ADD_COLUMNS_OR_IGNORE

    private val castCompatibleTypes =
        typeMismatchPolicy == TypeMismatchPolicy.CAST_COMPATIBLE_OR_ERROR ||
            typeMismatchPolicy == TypeMismatchPolicy.CAST_COMPATIBLE_OR_IGNORE
    private val castAlways =
        typeMismatchPolicy == TypeMismatchPolicy.CAST_ALWAYS
    private val ignoreTypes =
        typeMismatchPolicy == TypeMismatchPolicy.IGNORE ||
            typeMismatchPolicy == TypeMismatchPolicy.CAST_COMPATIBLE_OR_IGNORE

    private val pad = charVarcharPolicy == CharVarcharPolicy.PAD || charVarcharPolicy == CharVarcharPolicy.PAD_AND_TRUNCATE
    private val truncate = charVarcharPolicy == CharVarcharPolicy.TRUNCATE || charVarcharPolicy == CharVarcharPolicy.PAD_AND_TRUNCATE

    /**
      * Helper method for conforming a given schema to a target schema. This will project the given schema and also
      * add missing columns (which are filled with NULL values)
      * @param inputSchema - Denotes the input schema to be conformed
      * @return
      */
    def transform(inputSchema:StructType) : Seq[Column] = {
        if (columnMismatchPolicy != ColumnMismatchPolicy.IGNORE || typeMismatchPolicy != TypeMismatchPolicy.IGNORE || charVarcharPolicy != CharVarcharPolicy.IGNORE) {
            conformStruct(schema, inputSchema, "")
        }
        else {
            inputSchema.map(f => col(f.name))
        }
    }

    /**
      * Helper method for conforming a given schema to a target schema. This will project the given schema and also
      * add missing columns (which are filled with NULL values)
      * @param df - Denotes the input DataFrame
      * @return
      */
    def transform(df:DataFrame) : DataFrame = {
        if (columnMismatchPolicy != ColumnMismatchPolicy.IGNORE || typeMismatchPolicy != TypeMismatchPolicy.IGNORE || charVarcharPolicy != CharVarcharPolicy.IGNORE) {
            val unifiedColumns = transform(df.schema)
            df.select(unifiedColumns: _*)
        }
        else {
            df
        }
    }


    private def applyType(col:Column, field:StructField) : Column = {
        field.dataType match {
            case CharType(n) =>
                if (pad && truncate)
                    rpad(col.cast(StringType), n, " ")
                else if (pad)
                    when(length(col.cast(StringType)) > lit(n), col.cast(StringType))
                        .otherwise(rpad(col.cast(StringType), n, " "))
                else if (truncate)
                    substring(col.cast(StringType), 0, n)
                else
                    col.cast(StringType)
            case VarcharType(n) =>
                if (truncate)
                    substring(col.cast(StringType), 0, n)
                else
                    col.cast(StringType)
            case _ => col.cast(field.dataType)
        }
    }

    private def conformField(requiredField:StructField, inputField:StructField, prefix:String) : Column = {
        val inputType = inputField.dataType
        val field = requiredField.dataType match {
            // Simple match: DataType is already correct
            case `inputType` => col(prefix + requiredField.name)
            case st:StructType =>
                val columns = conformStruct(st, inputType.asInstanceOf[StructType], prefix + requiredField.name + ".")
                if (requiredField.nullable) {
                    nullable_struct(columns: _*)
                }
                else {
                    struct(columns: _*)
                }
            // Arrays are not completely supported...
            case _:ArrayType => col(prefix + requiredField.name)
            // Type mismatch, use policy to decide how to handle
            case _:DataType =>
                if (castAlways)
                    applyType(col(prefix + requiredField.name), requiredField)
                else if (castCompatibleTypes && coerce(requiredField.dataType, inputType) == requiredField.dataType)
                    applyType(col(prefix + requiredField.name), requiredField)
                else if (ignoreTypes)
                    col(prefix + requiredField.name)
                else
                    throw new SchemaMismatchException(s"Column ${prefix}.${requiredField.name} has type '$inputType', but required is '${requiredField.dataType}'")
        }

        // Apply comments and metadata
        val metadata = mergeMetadata(requiredField, inputField)
        alias(field, requiredField.name, metadata)
    }

    private def conformStruct(requiredSchema:StructType, inputSchema:StructType, prefix:String) : Seq[Column] = {
        val allColumns = {
            if (removeColumns) {
                // Allow to remove columns from input schema, so we simply restrict to requested columns
                requiredSchema.fields
            }
            else {
                // Here we are not allowed to remove input columns.
                // ADD_OR_ERROR | ADD_OR_IGNORE | IGNORE
                val allFieldNames = (requiredSchema.fields.map(_.name) ++ inputSchema.map(_.name)).map(_.toLowerCase(Locale.ROOT)).distinct
                val requiredFieldsByName = MapIgnoreCase(requiredSchema.map(f => f.name -> f))
                val inputFieldsByName = MapIgnoreCase(inputSchema.map(f => f.name -> f))
                allFieldNames.map { f =>
                    requiredFieldsByName.get(f)
                        .getOrElse {
                            // If required schema does not contain an input column
                            //    => add input column to result if to be ignored
                            //    => error if not to be ignored
                            if (ignoreColumns)
                                inputFieldsByName(f)
                            else
                                throw new SchemaMismatchException(s"Unexpected column '${prefix}.${inputFieldsByName(f).name}' on input side")
                        }
                }
            }
        }

        val inputFields = MapIgnoreCase(inputSchema.fields.map(field => field.name -> field))
        allColumns.flatMap { field =>
            inputFields.get(field.name)
                .map(f => conformField(field, f, prefix))
                .orElse {
                    if (addColumns)
                        Some(alias(applyType(lit(null), field), field.name, field.metadata))
                    else if (ignoreColumns)
                        None
                    else
                        throw new SchemaMismatchException(s"Missing column '${prefix}.${field.name}' on input side'")
                }
        }.toSeq
    }

    private def mergeMetadata(requiredField:StructField, inputField:StructField) : Metadata = {
        val metadata = requiredField.metadata
        val builder = new MetadataBuilder()
            .withMetadata(requiredField.metadata)

        // Try to get comment from input if no comment is provided in schema itself
        if (!metadata.contains("comment")) {
            inputField.getComment()
                .foreach(c => builder.putString("comment", c))
        }

        // Preserve extended string type info
        requiredField.dataType match {
            case VarcharType(_) if requiredField.dataType != inputField.dataType =>
                builder.putString(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY, requiredField.dataType.catalogString)
            case CharType(_) if requiredField.dataType != inputField.dataType =>
                builder.putString(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY, requiredField.dataType.catalogString)
            case _ =>
        }

        builder.build()
    }

    private def alias(col:Column, alias:String, metadata: Metadata) : Column = {
        SparkShim.alias(col, alias, metadata, Seq("collation", "charset"))
    }
}
