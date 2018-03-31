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

package com.dimajix.flowman.spec.schema

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructField

import com.dimajix.flowman.execution.Context


class Field {
    @JsonProperty(value="name", required = true) private var _name: String = _
    @JsonProperty(value="type", required = false) private var _type: FieldType = _
    @JsonProperty(value="nullable", required = true) private var _nullable: String = "true"
    @JsonProperty(value="description", required = false) private var _description: String = _
    @JsonProperty(value="granularity", required = false) private var _granularity: String = _

    def name : String = _name
    def ftype : FieldType = _type
    def nullable : Boolean = _nullable.toBoolean
    def description(implicit context: Context) : String = context.evaluate(_description)
    def granularity(implicit context: Context) : String = context.evaluate(_granularity)

    def sparkType : DataType = _type.sparkType
    def sparkField : StructField = StructField(name, sparkType, nullable)

    def interpolate(value: FieldValue)(implicit context: Context) : Iterable[Any] = _type.interpolate(value, granularity)
}
