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

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType


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
      * Helper method for applying an optional schema, as specified by the SourceTable
      *
      * @param df
      * @param schema
      * @return
      */
    def applySchema(df:DataFrame, schema:StructType) : DataFrame = {
        // Apply requested Schema
        if (schema != null)
            df.select(schema.map(field => col(field.name).cast(field.dataType)):_*)
        else
            df
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
      * Removes all meta data from a Spark schema. Useful for comparing results in unit tests
      * @param schema
      * @return
      */
    def dropMetadata(schema:StructType) : StructType = {
        def processType(dataType:DataType) : DataType = {
            dataType match {
                case st:StructType => dropMetadata(st)
                case ar:ArrayType => ArrayType(processType(ar.elementType), ar.containsNull)
                case mt:MapType => MapType(processType(mt.keyType), processType(mt.valueType), mt.valueContainsNull)
                case dt:DataType => dt
            }
        }
        val fields = schema.fields.map { field =>
            StructField(field.name, processType(field.dataType), field.nullable)
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
