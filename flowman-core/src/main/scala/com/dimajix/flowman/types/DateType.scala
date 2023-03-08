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

import java.sql.Date
import java.time.Duration
import java.time.LocalDate

import org.apache.spark.sql.types.DataType


case object DateType extends FieldType {
    override def sparkType : DataType = org.apache.spark.sql.types.DateType

    override def parse(value:String, granularity: Option[String]=None) : Date = {
        if (granularity.nonEmpty) {
            val step = Duration.parse(granularity.get).getSeconds/(24*60*60)
            val day = Date.valueOf(value).toLocalDate.toEpochDay / step * step
            Date.valueOf(LocalDate.ofEpochDay(day))
        }
        else {
            Date.valueOf(value)
        }
    }
    override def interpolate(value: FieldValue, granularity:Option[String]=None) : Iterable[Date] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(v => parse(v, granularity))
            case RangeValue(start,end,step) => {
                val startDate = Date.valueOf(start).toLocalDate.toEpochDay
                val endDate = Date.valueOf(end).toLocalDate.toEpochDay

                val result = if (step.nonEmpty) {
                    val range = startDate.until(endDate).by(Duration.parse(step.get).getSeconds / (24*60*60))
                    if (granularity.nonEmpty) {
                        val mod = Duration.parse(granularity.get).getSeconds / (24*60*60)
                        range.map(_ / mod * mod).distinct
                    }
                    else {
                        range
                    }
                }
                else if (granularity.nonEmpty) {
                    val mod = Duration.parse(granularity.get).getSeconds / (24*60*60)
                    (startDate / mod * mod).until(endDate / mod * mod).by(mod)
                }
                else {
                    startDate.until(endDate)
                }
                result.map(x => Date.valueOf(LocalDate.ofEpochDay(x)))
            }
        }
    }
}
