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

/**
  * This type represents a SQL Char. This type is not directly supported in Spark, but has to be mapped to a
  * StringType instead. Nevertheless the type is still useful for schema declarations.
  *
  * @param length
  */
case class CharType(length: Int) extends FieldType {
    override def typeName: String = s"char($length)"
    override def sparkType : DataType = org.apache.spark.sql.types.StringType
    override def sqlType : String = s"char($length)"

    override def parse(value:String, granularity: String) : String = {
        if (granularity != null && granularity.nonEmpty)
            throw new UnsupportedOperationException("Char types cannot have a granularity")
        value
    }
    override def interpolate(value: FieldValue, granularity:String) : Iterable[String] = {
        value match {
            case SingleValue(v) => Seq(v)
            case ArrayValue(values) => values.toSeq
            case RangeValue(start,end,step) => ???
        }
    }
}
