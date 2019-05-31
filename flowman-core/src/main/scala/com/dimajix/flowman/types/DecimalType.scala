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


object DecimalType {
    val MAX_PRECISION = 38
    val MAX_SCALE = 38
    val SYSTEM_DEFAULT: DecimalType = DecimalType(MAX_PRECISION, 18)
    val USER_DEFAULT: DecimalType = DecimalType(10, 0)
}
case class DecimalType(precision: Int, scale: Int) extends FieldType {
    override def typeName : String = s"decimal($precision,$scale)"
    override def sparkType : DataType = org.apache.spark.sql.types.DecimalType(precision, scale)
    override def sqlType : String = s"decimal($precision,$scale)"

    override def parse(value:String, granularity: String) : Any = {
        if (granularity != null || granularity.nonEmpty)
            throw new UnsupportedOperationException
        new java.math.BigDecimal(value)
    }
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(v => parse(v, granularity))
            case RangeValue(start, end, step) => throw new UnsupportedOperationException
        }
    }
}
