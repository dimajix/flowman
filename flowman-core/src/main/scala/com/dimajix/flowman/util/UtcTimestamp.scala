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

package com.dimajix.flowman.util

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter


object UtcTimestamp {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.S]").withZone(ZoneOffset.UTC)

    /**
      * Parses a string as a UTC timestamp and returns a UtcTimestamp object
      * @param value
      * @return
      */
    def parse(value:String) : UtcTimestamp = {
        val msecs = LocalDateTime.parse(value, formatter).toEpochSecond(ZoneOffset.UTC) * 1000l
        new UtcTimestamp(msecs)
    }

    /**
      * Parses a string as a UTC timestamp and returns the epoch in seconds
      * @param value
      * @return
      */
    def toEpochSeconds(value:String) : Long = {
        LocalDateTime.parse(value, formatter).toEpochSecond(ZoneOffset.UTC)
    }
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

    override def toLocalDateTime: LocalDateTime = {
        LocalDateTime.ofEpochSecond(getTime() / 1000, 0, ZoneOffset.UTC)
    }
}
