/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.model

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructField

import com.dimajix.flowman.types._


object PartitionField {
    def fromSpark(field:org.apache.spark.sql.types.StructField) : PartitionField = {
        PartitionField(
            field.name,
            FieldType.of(field.dataType),
            field.getComment()
        )
    }
}


/**
  * A PartitionField is a special field used for specifying partition columns. In addition to normal fields,
  * it also supports interpolation, but it lacks the capability of being nullable.
  */
case class PartitionField(
    name: String,
    ftype: FieldType,
    description: Option[String] = None,
    granularity: Option[String] = None
) {
    def field : Field = Field(name, ftype, false, description)

    def sparkType : DataType = ftype.sparkType
    def catalogType : DataType = ftype.catalogType
    def sparkField : StructField = StructField(name, sparkType, false)
    def catalogField : StructField = StructField(name, catalogType, false)

    def parse(value: String) : Any = ftype.parse(value, granularity)
    def interpolate(value: FieldValue) : Iterable[Any] = ftype.interpolate(value, granularity)
}
