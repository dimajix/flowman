/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.util

import java.util.Locale

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType

import com.dimajix.flowman.types.FieldType


object SchemaUtils {
    def mapType(typeName:String) : DataType  = {
        typeName.toLowerCase() match {
            case "string" => StringType
            case "float" => FloatType
            case "double" => DoubleType
            case "short" => ShortType
            case "int" => IntegerType
            case "integer" => IntegerType
            case "long" => LongType
            case "date" => DateType
            case "timestamp" => TimestampType
            case _ => throw new RuntimeException(s"Unknown type $typeName")
        }
    }
    def createSchema(fields:Seq[(String,String)]) : StructType = {
        // When reading data from MCSV files, we need to specify our desired schema. Here
        // we create a Spark SQL schema definition from the fields list in the specification
        def mapField(fieldName:String, typeName:String) : StructField = {
            StructField(fieldName, mapType(typeName), true)
        }

        StructType(fields.map { case (name:String,typ:String) => mapField(name, typ) } )
    }

    /**
      * Helper method for applying an optional schema to a given DataFrame. This will apply the types and order
      * of the target schema. Missing fields will be imputed by NULL values.
      *
      * @param df
      * @param schema
      * @return
      */
    def applySchema(df:DataFrame, schema:Option[StructType]) : DataFrame = {
        require(df != null)
        require(schema != null)

        val dfFieldsByName = df.schema.map(f => f.name.toLowerCase(Locale.ROOT) -> f).toMap
        schema
            .map { schema =>
                val columns = schema.map { field =>
                    dfFieldsByName.get(field.name.toLowerCase(Locale.ROOT))
                        .map(_ => df(field.name).cast(field.dataType).as(field.name, field.metadata))
                        .getOrElse(lit(null).cast(field.dataType).as(field.name, field.metadata))
                }
                df.select(columns: _*)
            }
            .getOrElse(df)
    }

    /**
      * Finds a specific field in a schema
      * @param struct
      * @param name
      * @return
      */
    def find(struct:StructType, name:String) : Option[StructField] = {
        def findField(field:StructField, head:String, tail:Seq[String]) : Option[StructField] = {
            field.dataType match {
                case st:StructType => findStruct(st, head, tail)
                case at:ArrayType => findField(StructField("element", at.elementType), head, tail)
                case _ => throw new NoSuchElementException(s"Cannot descend field ${field.name} - it is neither struct not array")
            }
        }
        def findStruct(struct:StructType, head:String, tail:Seq[String]) : Option[StructField] = {
            struct.fields
                .find(_.name.toLowerCase(Locale.ROOT) == head)
                .flatMap { field =>
                    if (tail.isEmpty)
                        Some(field)
                    else
                        findField(field, tail.head, tail.tail)
                }
        }

        val segments = name.toLowerCase(Locale.ROOT).split('.')
        findStruct(struct, segments.head, segments.tail)
    }

    /**
      * Merges two fields into a common field
      * @param newField
      * @param existingField
      * @return
      */
    def merge(newField:StructField, existingField:StructField) : StructField = {
        val nullable = existingField.nullable || existingField.dataType == NullType ||
            newField.nullable || newField.dataType == NullType
        val dataType = coerce(existingField.dataType, newField.dataType)
        val result = existingField.copy(dataType=dataType, nullable=nullable)

        val comment = existingField.getComment().orElse(newField.getComment())
        comment.map(result.withComment).getOrElse(result)
    }

    /**
      * Create a UNION of several schemas
      * @param schemas
      * @return
      */
    def union(schemas:Seq[StructType]) : StructType = {
        def commonField(newField:StructField, fields:Map[String,StructField]) = {
            val existingField = fields.getOrElse(newField.name.toLowerCase(Locale.ROOT), newField)
            merge(newField, existingField)
        }
        val allColumns = schemas.foldLeft(Map[String,StructField]())((columns, schema) => {
            val tableColumns = schema
                .map(field => field.name.toLowerCase(Locale.ROOT) -> commonField(field, columns))
                .toMap
            columns ++ tableColumns
        })

        // Create a common schema from collected columns
        StructType(allColumns.values.toSeq.sortBy(_.name.toLowerCase(Locale.ROOT)))
    }

    /**
      * Performs type coercion, i.e. find the tightest common data type that can be used to contain values of two
      * other incoming types
      * @param left
      * @param right
      * @return
      */
    def coerce(left:DataType, right:DataType) : DataType = {
        com.dimajix.flowman.types.SchemaUtils.coerce(FieldType.of(left), FieldType.of(right)).catalogType
    }

    /**
      * Verify is a given source field is compatible with a given target field, i.e. if a safe conversion is
      * possible from a source field to a target field
      * @param sourceField
      * @param targetField
      * @return
      */
    def isCompatible(sourceField:StructField, targetField:StructField) : Boolean = {
        if (sourceField.name.toLowerCase(Locale.ROOT) != targetField.name.toLowerCase(Locale.ROOT)) {
            false
        }
        else {
            val sourceNullable = sourceField.nullable || sourceField.dataType == NullType
            val targetNullable = targetField.nullable || targetField.dataType == NullType
            if (!sourceNullable && targetNullable) {
                false
            }
            else {
                val coercedType = coerce(sourceField.dataType, targetField.dataType)
                coercedType == sourceField.dataType
            }
        }
    }

    /**
      * Verify if a given source schema can be safely cast to a target schema.
      * @param sourceSchema
      * @param targetSchema
      * @return
      */
    def isCompatible(sourceSchema:StructType, targetSchema:StructType) : Boolean = {
        val targetFieldsByName = targetSchema.fields.map(field => (field.name.toLowerCase(Locale.ROOT), field)).toMap
        sourceSchema.forall(field =>
            targetFieldsByName.get(field.name.toLowerCase(Locale.ROOT))
                .exists(actual => isCompatible(actual, field))
        )
    }

    /**
      * Truncate comments to maximum length. Maybe required for Hive tables
      * @param schema
      * @param maxLength
      * @return
      */
    def truncateComments(schema:StructType, maxLength:Int) : StructType = {
        def processType(dataType:DataType) : DataType = {
            dataType match {
                case st:StructType => truncateComments(st, maxLength)
                case ar:ArrayType => ar.copy(elementType = processType(ar.elementType))
                case mt:MapType => mt.copy(keyType = processType(mt.keyType), valueType = processType(mt.valueType))
                case dt:DataType => dt
            }
        }
        def truncate(field:StructField) : StructField = {
            val metadata = field.getComment()
                .map(comment => new MetadataBuilder()
                    .withMetadata(field.metadata)
                    .putString("comment", comment.take(maxLength))
                    .build()
                ).getOrElse(field.metadata)
            val dataType = processType(field.dataType)
            field.copy(dataType = dataType, metadata = metadata)
        }
        val fields = schema.fields.map(truncate)
        StructType(fields)
    }

    /**
      * Removes all meta data from a Spark schema. Useful for comparing results in unit tests
      * @param schema
      * @return
      */
    def dropMetadata(schema:StructType) : StructType = {
        def processType(dataType:DataType) : DataType = {
            dataType match {
                case st:StructType => dropMetadata(st)
                case ar:ArrayType => ar.copy(elementType = processType(ar.elementType))
                case mt:MapType => mt.copy(keyType = processType(mt.keyType), valueType = processType(mt.valueType))
                case dt:DataType => dt
            }
        }

        val fields = schema.fields.map { field =>
            field.copy(dataType = processType(field.dataType), metadata = Metadata.empty)
        }
        StructType(fields)
    }

    /**
      * Converts the given Spark schema to a lower case schema
      * @param schema
      * @return
      */
    def toLowerCase(schema:StructType) : StructType = {
        StructType(schema.fields.map(toLowerCase))
    }
    private def toLowerCase(field:StructField) : StructField = {
        StructField(field.name.toLowerCase(Locale.ROOT), toLowerCase(field.dataType), field.nullable, field.metadata)
    }
    private def toLowerCase(dtype:DataType) : DataType = {
        dtype match {
            case struct:StructType => toLowerCase(struct)
            case array:ArrayType => ArrayType(toLowerCase(array.elementType),array.containsNull)
            case map:MapType => MapType(toLowerCase(map.keyType), toLowerCase(map.valueType), map.valueContainsNull)
            case dt:DataType => dt
        }
    }
}
