/*
 * Copyright (C) 2018 The Flowman Authors
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

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.types.FieldType


object SparkSchemaUtils {
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
            if (sourceNullable && !targetNullable) {
                false
            }
            else {
                val coercedType = coerce(sourceField.dataType, targetField.dataType)
                coercedType == targetField.dataType
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
        val targetFieldsByName = targetSchema.map(field => (field.name.toLowerCase(Locale.ROOT), field)).toMap
        sourceSchema.forall(field =>
            targetFieldsByName.get(field.name.toLowerCase(Locale.ROOT))
                .exists(actual => isCompatible(field, actual))
        )
    }
}
