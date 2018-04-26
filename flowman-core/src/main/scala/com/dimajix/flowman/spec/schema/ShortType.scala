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

import org.apache.spark.sql.types.DataType


case object ShortType extends FieldType {
    override def sparkType : DataType = org.apache.spark.sql.types.ShortType

    override def parse(value:String, granularity: String) : Any =  {
        if (granularity != null && granularity.nonEmpty)
            (value.toShort / granularity.toShort * granularity.toShort).toShort
        else
            value.toShort
    }
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(parse(_, granularity))
            case RangeValue(start,end) => {
                if (granularity != null && granularity.nonEmpty)
                    parse(start, granularity).asInstanceOf[Short] until parse(end,granularity).asInstanceOf[Short] by granularity.toShort map(_.toShort)
                else
                    start.toShort until end.toShort map(_.toShort)
            }
        }
    }
}
