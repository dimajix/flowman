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

package com.dimajix.spark.sql

import java.util.Locale

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.rpad
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.CharType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.VarcharType

import com.dimajix.spark.features.hiveVarcharSupported


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
    def applySchema(df:DataFrame, schema:Option[StructType], insertNulls:Boolean=true) : DataFrame = {
        require(df != null)
        require(schema != null)

        def applyType(col:Column, field:StructField) : Column = {
            field.dataType match {
                case CharType(n) => rpad(col.cast(StringType), n, " ").as(field.name, field.metadata)
                case VarcharType(n) => substring(col.cast(StringType), 0, n).as(field.name, field.metadata)
                case _ => col.cast(field.dataType).as(field.name, field.metadata)
            }
        }

        def applySchema(df:DataFrame, schema:StructType, insertNulls:Boolean) : DataFrame = {
            val dfFieldsByName = df.schema.map(f => f.name.toLowerCase(Locale.ROOT) -> f).toMap
            val columns = schema.map { field =>
                val col = dfFieldsByName.get(field.name.toLowerCase(Locale.ROOT))
                    .map(_ => df(field.name))
                    .getOrElse {
                        if (!insertNulls)
                            throw new IllegalArgumentException(s"Missing column '${field.name}' in input DataFrame")
                        lit(null)
                    }
                applyType(col, field)
            }
            df.select(columns: _*)
        }

        schema match {
            case Some(s) => applySchema(df, s, insertNulls)
            case None => df
        }
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
     * This will normalize a given schema in the sense that all field names are converted to lowercase and all
     * metadata is stripped except the comments. The function will also replace all CHAR and VARCHAR columns to
     * STRING columns.
     * @param schema
     * @return
     */
    def normalize(schema:StructType) : StructType = {
        StructType(schema.fields.map(normalize))
    }
    def normalize(field:StructField) : StructField = {
        val f = StructField(field.name.toLowerCase(Locale.ROOT), normalize(field.dataType), field.nullable)
        field.getComment().map(c => f.withComment(c)).getOrElse(f)
    }
    private def normalize(dtype:DataType) : DataType = {
        dtype match {
            case struct:StructType => normalize(struct)
            case array:ArrayType => ArrayType(normalize(array.elementType),array.containsNull)
            case map:MapType => MapType(normalize(map.keyType), normalize(map.valueType), map.valueContainsNull)
            case _:CharType => StringType
            case _:VarcharType => StringType
            case dt:DataType => dt
        }
    }

    /**
     * Replaces all occurances of VarChar and Char types by String types.
     * @param schema
     * @return
     */
    def replaceCharVarchar(schema:StructType) : StructType = {
        StructType(schema.fields.map(replaceCharVarchar))
    }
    def replaceCharVarchar(field:StructField) : StructField = {
        field.copy(dataType = replaceCharVarchar(field.dataType))
    }
    private def replaceCharVarchar(dtype:DataType) : DataType = {
        dtype match {
            case struct:StructType => replaceCharVarchar(struct)
            case array:ArrayType => ArrayType(replaceCharVarchar(array.elementType),array.containsNull)
            case map:MapType => MapType(replaceCharVarchar(map.keyType), replaceCharVarchar(map.valueType), map.valueContainsNull)
            case _:CharType => StringType
            case _:VarcharType => StringType
            case dt:DataType => dt
        }
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
