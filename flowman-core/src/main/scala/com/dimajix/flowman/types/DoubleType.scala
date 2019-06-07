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


case object DoubleType extends FieldType {
    override def sparkType : DataType = org.apache.spark.sql.types.DoubleType

    override def parse(value:String, granularity:Option[String]=None) : Double = {
        if (granularity.nonEmpty) {
            val step = granularity.get.toDouble
            val v = value.toDouble
            v - (v % step)
        }
        else {
            value.toDouble
        }
    }
    override def interpolate(value: FieldValue, granularity:Option[String]=None) : Iterable[Double] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(v => parse(v,granularity))
            case RangeValue(start,end,step) => {
                if (step.nonEmpty) {
                    val range = start.toDouble.until(end.toDouble).by(step.get.toDouble)
                    if (granularity.nonEmpty) {
                        val mod = granularity.get.toDouble
                        range.map(x => math.floor(x / mod) * mod).distinct
                    }
                    else {
                        range
                    }
                }
                else if (granularity.nonEmpty) {
                    val mod = granularity.get.toDouble
                    start.toDouble.until(end.toDouble).by(mod).map(x => math.floor(x / mod) * mod)
                }
                else {
                    start.toDouble.until(end.toDouble).by(1.0)
                }
            }
        }
    }
}
