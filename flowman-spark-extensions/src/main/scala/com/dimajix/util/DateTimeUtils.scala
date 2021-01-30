/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

package com.dimajix.util

import java.sql.Date
import java.sql.Timestamp
import java.util.TimeZone

import scala.annotation.tailrec

import javax.xml.bind.DatatypeConverter
import org.apache.spark.sql.SparkShim


object DateTimeUtils {
    final val SECONDS_PER_MINUTE = 60L
    final val SECONDS_PER_HOUR = 60 * 60L
    final val SECONDS_PER_DAY = SECONDS_PER_HOUR * 24L

    final val MILLIS_PER_SECOND = 1000L
    final val MILLIS_PER_MINUTE = 1000L * 60L
    final val MILLIS_PER_HOUR = SECONDS_PER_HOUR * 1000L
    final val MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L

    final val MICROS_PER_MILLIS = 1000L
    final val MICROS_PER_SECOND = MICROS_PER_MILLIS * MILLIS_PER_SECOND
    final val MICROS_PER_MINUTE = MICROS_PER_MILLIS * MILLIS_PER_MINUTE
    final val MICROS_PER_HOUR = MICROS_PER_MILLIS * MILLIS_PER_HOUR
    final val MICROS_PER_DAY = MICROS_PER_SECOND * SECONDS_PER_DAY

    final val NANOS_PER_SECOND = MICROS_PER_SECOND * 1000L
    final val NANOS_PER_MICROS = 1000L

    @tailrec
    def stringToTime(s: String): java.util.Date = {
        val indexOfGMT = s.indexOf("GMT")
        if (indexOfGMT != -1) {
            // ISO8601 with a weird time zone specifier (2000-01-01T00:00GMT+01:00)
            val s0 = s.substring(0, indexOfGMT)
            val s1 = s.substring(indexOfGMT + 3)
            // Mapped to 2000-01-01T00:00+01:00
            stringToTime(s0 + s1)
        } else if (!s.contains('T')) {
            // JDBC escape string
            if (s.contains(' ')) {
                Timestamp.valueOf(s)
            } else {
                Date.valueOf(s)
            }
        } else {
            DatatypeConverter.parseDateTime(s).getTime()
        }
    }

    def millisToDays(millisUtc: Long): Int = {
        org.apache.spark.sql.catalyst.util.DateTimeUtils.millisToDays(millisUtc)
    }

    def millisToDays(millisUtc: Long, timeZone: TimeZone): Int = {
        SparkShim.millisToDays(millisUtc, timeZone)
    }
}
