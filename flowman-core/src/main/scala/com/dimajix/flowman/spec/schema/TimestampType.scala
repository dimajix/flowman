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

import java.sql.Timestamp
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset

import org.apache.spark.sql.types.DataType


case object TimestampType extends FieldType {
    override def sparkType : DataType = org.apache.spark.sql.types.TimestampType

    override def parse(value:String, granularity: String) : Any = {
        if (granularity != null && granularity.nonEmpty) {
            val secs = LocalDateTime.parse(value).toEpochSecond(ZoneOffset.UTC)
            val step = Duration.parse(granularity).getSeconds
            new Timestamp(secs / step * step * 1000l)
        }
        else {
            new Timestamp(LocalDateTime.parse(value).toEpochSecond(ZoneOffset.UTC) * 1000l)
        }
    }
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(parse(_, granularity))
            case RangeValue(start,end) => {
                val step = if (granularity != null && granularity.nonEmpty)
                    Duration.parse(granularity).getSeconds
                else
                    1
                val startDate = LocalDateTime.parse(start).toEpochSecond(ZoneOffset.UTC) / step * step
                val endDate = LocalDateTime.parse(end).toEpochSecond(ZoneOffset.UTC) / step * step
                startDate until endDate by step map(x => new Timestamp(x * 1000l))
            }
        }
    }
}
