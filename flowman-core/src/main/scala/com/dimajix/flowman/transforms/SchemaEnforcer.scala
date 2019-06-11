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

package com.dimajix.flowman.transforms

import java.util.Locale

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.dimajix.spark.functions.nullable_struct
import com.dimajix.flowman.util.SchemaUtils


object SchemaEnforcer {
    def apply(columns:Seq[(String,String)]) : SchemaEnforcer = {
        val schema = StructType(columns.map(nt => StructField(nt._1, SchemaUtils.mapType(nt._2))))
        SchemaEnforcer(schema)
    }
}


case class SchemaEnforcer(schema:StructType) {
    /**
      * Helper method for conforming a given schema to a target schema. This will project the given schema and also
      * add missing columns (which are filled with NULL values)
      * @param inputSchema - Denotes the input schema to be conformed
      * @return
      */
    def transform(inputSchema:StructType) : Seq[Column] = {
        def conformField(requiredField:StructField, inputType:DataType, prefix:String) : Column = {
            requiredField.dataType match {
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
                case _:DataType => col(prefix + requiredField.name).cast(requiredField.dataType)
            }
        }

        def conformStruct(requiredSchema:StructType, inputSchema:StructType, prefix:String) : Seq[Column] = {
            val inputFields = inputSchema.fields.map(field => (field.name.toLowerCase(Locale.ROOT), field)).toMap
            requiredSchema.fields.map { field =>
                inputFields.get(field.name.toLowerCase(Locale.ROOT))
                    .map(f => conformField(field, f.dataType, prefix))
                    .getOrElse(lit(null).cast(field.dataType))
                    .as(field.name)
            }.toSeq
        }

        conformStruct(schema, inputSchema, "")
    }

    /**
      * Helper method for conforming a given schema to a target schema. This will project the given schema and also
      * add missing columns (which are filled with NULL values)
      * @param df - Denotes the input DataFrame
      * @return
      */
    def transform(df:DataFrame) : DataFrame = {
        val unifiedColumns = transform(df.schema)
        df.select(unifiedColumns:_*)
    }
}
