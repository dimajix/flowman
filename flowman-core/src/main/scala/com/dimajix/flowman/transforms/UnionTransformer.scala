/*
 * Copyright 2019 Kaya Kupferschmidt
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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructField

import com.dimajix.flowman.{types => ftypes}


case class UnionTransformer() {
    /**
      * Transform a Spark DataFrame
      * @param input
      * @return
      */
    def transformDataFrames(input:Seq[DataFrame]) : DataFrame = {
        val schemas = input.map(_.schema)
        val allColumns = schemas.foldLeft(UnionSchema[StructField]())((union, schema) => {
            schema.foldLeft(union)((union,field) => union.withField(field.name, field, mergeFields))
        }).fields

        val projectedDf = input.map { df =>
            // Get set of available field names
            val fields = df.schema.fields.map(_.name.toLowerCase(Locale.ROOT)).toSet
            // Either select corresponding field or NULL
            df.select(allColumns.map(col =>
                if (fields.contains(col.name.toLowerCase(Locale.ROOT)))
                    df(col.name).cast(col.dataType)
                else
                    lit(null).cast(col.dataType).as(col.name)
            ):_*)
        }

        // Union all DataFrames into the result
        projectedDf.reduce(_.union(_))
    }

    /**
      * Transform a Flowman schema
      * @param input
      * @return
      */
    def transformSchemas(input:Seq[ftypes.StructType]) : ftypes.StructType = {
        val allColumns = input.foldLeft(UnionSchema[ftypes.Field]())((union, schema) => {
            schema.fields.foldLeft(union)((union,field) => union.withField(field.name, field, mergeFields))
        })

        // Fix "nullable" property of columns not present in all input schemas
        val allColumnNames = allColumns.fieldsByName.keySet
        val nullableColumns = input.foldLeft(allColumns)((union, schema) => {
            val fields = schema.fields.map(_.name.toLowerCase(Locale.ROOT)).toSet
            allColumnNames.foldLeft(union)((union,field) =>
                if (fields.contains(field))
                    union
                else
                    union.withField(field, union.fieldsByName(field).copy(nullable = true))
            )
        })

        ftypes.StructType(nullableColumns.fields)
    }

    private case class UnionSchema[T](
        fieldsByName:Map[String,T] = Map[String,T](),
        fieldNames:Seq[String] = Seq()
    ) {
        def fields:Seq[T] = fieldNames.map(name => fieldsByName(name))

        def withField(name:String, newField:T) : UnionSchema[T] = {
            val lowerName = name.toLowerCase(Locale.ROOT)
            val newFieldByName = fieldsByName.updated(lowerName, newField)
            val newFieldNames = if (fieldsByName.contains(lowerName))
                fieldNames
            else
                fieldNames :+ lowerName

            UnionSchema(
                newFieldByName,
                newFieldNames
            )
        }
        def withField(name:String, field:T, merge:(T,T) => T) : UnionSchema[T] = {
            val lowerName = name.toLowerCase(Locale.ROOT)
            val newField = fieldsByName.get(lowerName).map(merge(_, field)).getOrElse(field)
            val newFieldByName = fieldsByName.updated(lowerName, newField)
            val newFieldNames = if (fieldsByName.contains(lowerName))
                    fieldNames
                else
                    fieldNames :+ lowerName

            UnionSchema(
                newFieldByName,
                newFieldNames
            )
        }
    }

    private def mergeFields(newField:StructField, existingField:StructField) : StructField = {
        val nullable = existingField.nullable || newField.nullable
        val dataType = if (existingField.dataType == org.apache.spark.sql.types.NullType) newField.dataType else existingField.dataType
        val result = existingField.copy(dataType=dataType, nullable=nullable)

        val comment = existingField.getComment().orElse(newField.getComment())
        comment.map(result.withComment).getOrElse(result)
    }
    private def mergeFields(newField:ftypes.Field, existingField:ftypes.Field) : ftypes.Field = {
        val nullable = existingField.nullable || newField.nullable
        val dataType = if (existingField.ftype == ftypes.NullType) newField.ftype else existingField.ftype
        val description = existingField.description.orElse(newField.description)
        val default = existingField.default.orElse(newField.default)
        val size = existingField.size.orElse(newField.size)
        val format = existingField.format.orElse(newField.format)

        ftypes.Field(existingField.name, dataType, nullable, description, default, size, format)
    }
}
