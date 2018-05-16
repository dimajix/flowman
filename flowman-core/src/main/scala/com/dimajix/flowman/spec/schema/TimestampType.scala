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
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.types.DataType


object UtcTimestamp {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.S]").withZone(ZoneOffset.UTC)
}

/**
  * This is a helper class which uses UTC in its "toString" method as opposed to the java.sql.Timestamp class
  * which uses local date time
  * @param msecs
  */
class UtcTimestamp(msecs:Long) extends Timestamp(msecs) {
    import UtcTimestamp.formatter

    override def toString: String = {
        LocalDateTime.ofEpochSecond(getTime() / 1000, 0, ZoneOffset.UTC).format(formatter)
    }
}


/**
  * A FieldType that represents SQL Timestamps
  */
case object TimestampType extends FieldType {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.S]").withZone(ZoneOffset.UTC)

    override def sparkType : DataType = org.apache.spark.sql.types.TimestampType

    /**
      * Parses a String into a java.sql.Timestamp object. The string has to be of format "yyyy-MM-dd HH:mm:ss"
      * @param value
      * @param granularity
      * @return
      */
    override def parse(value:String, granularity: String) : Any = {
        if (granularity != null && granularity.nonEmpty) {
            val msecs = LocalDateTime.parse(value, formatter).toEpochSecond(ZoneOffset.UTC) * 1000l
            val step = Duration.parse(granularity).getSeconds * 1000l
            new UtcTimestamp(msecs / step * step)
        }
        else {
            val msecs = LocalDateTime.parse(value, formatter).toEpochSecond(ZoneOffset.UTC) * 1000l
            new UtcTimestamp(msecs)
        }
    }
    /**
      * Parses a FieldValue into a sequence of java.sql.Timestamp objects. The fields have to be of format
      * "yyyy-MM-dd HH:mm:ss"
      * @param value
      * @param granularity
      * @return
      */
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(parse(_, granularity))
            case RangeValue(start,end) => {
                val step = if (granularity != null && granularity.nonEmpty)
                    Duration.parse(granularity).getSeconds
                else
                    1
                val startDate = LocalDateTime.parse(start, formatter).toEpochSecond(ZoneOffset.UTC) / step * step
                val endDate = LocalDateTime.parse(end, formatter).toEpochSecond(ZoneOffset.UTC) / step * step
                startDate until endDate by step map(x => new UtcTimestamp(x * 1000l))
            }
        }
    }
}
