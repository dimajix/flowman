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


object DateTimeUtils {
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
        org.apache.spark.sql.catalyst.util.DateTimeUtils.millisToDays(millisUtc, timeZone)
    }
}
