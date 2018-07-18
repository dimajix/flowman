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

import java.time.Duration

import org.apache.spark.sql.types.DataType

import com.dimajix.flowman.util.UtcTimestamp


/**
  * A FieldType that represents SQL Timestamps
  */
case object TimestampType extends FieldType {
    override def sparkType : DataType = org.apache.spark.sql.types.TimestampType

    /**
      * Parses a String into a java.sql.Timestamp object. The string has to be of format "yyyy-MM-ddTHH:mm:ss"
      * @param value
      * @param granularity
      * @return
      */
    override def parse(value:String, granularity: String) : UtcTimestamp = {
        if (granularity != null && granularity.nonEmpty) {
            val msecs = UtcTimestamp.toEpochSeconds(value) * 1000l
            val step = Duration.parse(granularity).getSeconds * 1000l
            new UtcTimestamp(msecs / step * step)
        }
        else {
            UtcTimestamp.parse(value)
        }
    }
    /**
      * Parses a FieldValue into a sequence of java.sql.Timestamp objects. The fields have to be of format
      * "yyyy-MM-ddTHH:mm:ss"
      * @param value
      * @param granularity
      * @return
      */
    override def interpolate(value: FieldValue, granularity:String) : Iterable[UtcTimestamp] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(parse(_, granularity))
            case RangeValue(start,end,step) => {
                val startDate = UtcTimestamp.toEpochSeconds(start)
                val endDate = UtcTimestamp.toEpochSeconds(end)

                val result = if (step != null && step.nonEmpty) {
                    val range = startDate.until(endDate).by(Duration.parse(step).getSeconds)
                    if (granularity != null && granularity.nonEmpty) {
                        val mod = Duration.parse(granularity).getSeconds
                        range.map(_ / mod * mod).distinct
                    }
                    else {
                        range
                    }
                }
                else if (granularity != null && granularity.nonEmpty) {
                    val mod = Duration.parse(granularity).getSeconds
                    (startDate / mod * mod).until(endDate / mod * mod).by(mod)
                }
                else {
                    startDate.until(endDate)
                }
                result.map(x => new UtcTimestamp(x * 1000l))
            }
        }
    }
}
