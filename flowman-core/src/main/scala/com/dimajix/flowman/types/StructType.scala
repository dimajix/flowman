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

package com.dimajix.flowman.types

import org.apache.spark.sql.types.DataType
import org.codehaus.jackson.annotate.JsonProperty


case class StructType(
    @JsonProperty(value = "fields") fields:Seq[Field]
                     ) extends ContainerType {
    def this() = { this(Seq()) }
    override def sparkType : DataType = {
        org.apache.spark.sql.types.StructType(fields.map(_.sparkField))
    }
    override def sqlType : String = {
        "struct<" + fields.map(f => f.name + ":" + f.sqlType).mkString(",") + ">"
    }
    override def parse(value:String, granularity: String) : Any = ???
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = ???
}
