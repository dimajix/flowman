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


case object LongType extends FieldType {
    override def sparkType : DataType = org.apache.spark.sql.types.LongType

    override def parse(value:String, granularity: String) : Long = {
        if (granularity != null && granularity.nonEmpty)
            value.toLong / granularity.toLong * granularity.toLong
        else
            value.toLong
    }
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Long] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(parse(_, granularity))
            case RangeValue(start,end,step) => {
                if (step != null && step.nonEmpty) {
                    val range = start.toLong.until(end.toLong).by(step.toLong)
                    if (granularity != null && granularity.nonEmpty) {
                        val mod = granularity.toLong
                        range.map(_ / mod * mod).distinct
                    }
                    else {
                        range
                    }
                }
                else if (granularity != null && granularity.nonEmpty) {
                    val mod = granularity.toLong
                    (start.toLong  / mod * mod).until(end.toLong / mod * mod).by(mod)
                }
                else {
                    start.toLong.until(end.toLong)
                }
            }
        }
    }
}
