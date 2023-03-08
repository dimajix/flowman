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

import java.time.Duration

import org.apache.spark.sql.types.DataType


case object DurationType extends FieldType {
    override def sparkType : DataType = ???

    /**
      * Parses a String into a java.time.Duration object.
      * @param value
      * @param granularity
      * @return
      */
    override def parse(value:String, granularity: Option[String]=None) : Duration = {
        if (granularity.nonEmpty) {
            val secs = Duration.parse(value).getSeconds
            val step = Duration.parse(granularity.get).getSeconds
            Duration.ofSeconds(roundDown(secs, step))
        }
        else {
            Duration.parse(value)
        }
    }

    /**
      * Parses a FieldValue into a sequence of java.sql.Timestamp objects. The fields have to be of format
      * "yyyy-MM-dd HH:mm:ss"
      * @param value
      * @param granularity
      * @return
      */
    override def interpolate(value: FieldValue, granularity:Option[String]=None) : Iterable[Duration] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(parse(_, granularity))
            case RangeValue(start,end,step) => {
                val startDuration = Duration.parse(start).getSeconds
                val endDuration = Duration.parse(end).getSeconds

                val result = if (step.nonEmpty) {
                    val range = startDuration.until(endDuration).by(Duration.parse(step.get).getSeconds)
                    if (granularity.nonEmpty) {
                        val mod = Duration.parse(granularity.get).getSeconds
                        range.map(x => roundDown(x, mod)).distinct
                    }
                    else {
                        range
                    }
                }
                else if (granularity.nonEmpty) {
                    val mod = Duration.parse(granularity.get).getSeconds
                    roundDown(startDuration, mod).until(roundDown(endDuration, mod)).by(mod)
                }
                else {
                    startDuration.until(endDuration)
                }
                result.map(x => Duration.ofSeconds(x))
            }
        }
    }

    private def roundDown(secs:Long, granularity:Long) : Long = {
        secs / granularity * granularity
    }
}
