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


object Field {
    def apply(name:String, ftype:FieldType, nullable:Boolean=true, description:String="") : Field = {
        val field = new Field()
        field._name = name
        field._type = ftype
        field._nullable = nullable.toString
        field._description = description
        field
    }
}


class Field {
    @JsonProperty(value="name", required = true) private var _name: String = _
    @JsonProperty(value="type", required = false) private var _type: FieldType = _
    @JsonProperty(value="nullable", required = true) private var _nullable: String = "true"
    @JsonProperty(value="description", required = false) private var _description: String = _

    def name : String = _name
    def ftype : FieldType = _type
    def nullable : Boolean = _nullable.toBoolean
    def description(implicit context: Context) : String = context.evaluate(_description)

    def sqlType : String = _type.sqlType
    def sparkType : DataType = _type.sparkType
    def sparkField : StructField = StructField(name, sparkType, nullable)
}
