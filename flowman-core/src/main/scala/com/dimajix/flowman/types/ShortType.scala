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


case object ShortType extends FieldType {
    override def sparkType : DataType = org.apache.spark.sql.types.ShortType

    override def parse(value:String, granularity:Option[String]=None) : Short =  {
        if (granularity.nonEmpty)
            (value.toShort / granularity.get.toShort * granularity.get.toShort).toShort
        else
            value.toShort
    }
    override def interpolate(value: FieldValue, granularity:Option[String]=None) : Iterable[Short] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(parse(_, granularity))
            case RangeValue(start,end,step) => {
                if (step.nonEmpty) {
                    val range = start.toInt.until(end.toInt).by(step.get.toInt)
                    if (granularity.nonEmpty) {
                        val mod = granularity.get.toInt
                        range.map(_ / mod * mod).distinct.map(_.toShort)
                    }
                    else {
                        range.map(_.toShort)
                    }
                }
                else if (granularity.nonEmpty) {
                    val mod = granularity.get.toInt
                    (start.toInt / mod * mod).until(end.toInt / mod * mod).by(mod).map(_.toShort)
                }
                else {
                    start.toInt.until(end.toInt).map(_.toShort)
                }
            }
        }
    }
}
