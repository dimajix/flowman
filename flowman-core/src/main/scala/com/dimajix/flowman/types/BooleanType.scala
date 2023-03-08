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

package com.dimajix.flowman.types

import org.apache.spark.sql.types.DataType


case object BooleanType extends FieldType {
    override def sparkType : DataType = org.apache.spark.sql.types.BooleanType

    override def parse(value:String, granularity:Option[String]=None) : Boolean = {
        if (granularity.nonEmpty)
            throw new UnsupportedOperationException("Boolean types cannot have a granularity")
        value.toBoolean
    }
    override def interpolate(value: FieldValue, granularity:Option[String]=None) : Iterable[Boolean] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(v => parse(v, granularity))
            case RangeValue(start,end,step) => ???
        }
    }
}
