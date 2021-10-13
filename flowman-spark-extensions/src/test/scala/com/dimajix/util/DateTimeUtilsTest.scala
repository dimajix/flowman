/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import java.sql.Timestamp
import java.sql.{Date => SqlDate}
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.Date
import java.util.TimeZone

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class DateTimeUtilsTest extends AnyFlatSpec with Matchers {
    "DateTimeUtils" should "correct parse date and time" in {
        DateTimeUtils.stringToTime("2020-02-03") should be (SqlDate.valueOf("2020-02-03"))
        DateTimeUtils.stringToTime("2020-02-03 23:11:20") should be (Timestamp.valueOf("2020-02-03 23:11:20"))
        //DateTimeUtils.stringToTime("2000-01-01T22:33GMT+01:00")
        DateTimeUtils.stringToTime("2020-02-03T22:33:11GMT+05:00") should be (Date.from(Instant.from(ZonedDateTime.of(2020,2,3,22,33,11,0,ZoneId.of("GMT+05:00")))))
        //DateTimeUtils.stringToTime("2000-01-01T22:33")
        DateTimeUtils.stringToTime("2020-02-03T22:33:11") should be (Timestamp.valueOf("2020-02-03 22:33:11"))
        DateTimeUtils.stringToTime("2020-02-03T22:33:11+00:00") should be (Date.from(Instant.from(ZonedDateTime.of(2020,2,3,22,33,11,0,ZoneId.of("UTC")))))
    }

    it should "correctly convert milliseconds to days" in {
        val utc = TimeZone.getTimeZone("UTC")

        DateTimeUtils.millisToDays(0, utc) should be (0)
        DateTimeUtils.millisToDays(DateTimeUtils.MILLIS_PER_DAY, utc) should be (1)
        DateTimeUtils.millisToDays(DateTimeUtils.MILLIS_PER_DAY - 1, utc) should be (0)
        DateTimeUtils.millisToDays(DateTimeUtils.MILLIS_PER_DAY + 1, utc) should be (1)

        DateTimeUtils.millisToDays(-DateTimeUtils.MILLIS_PER_DAY, utc) should be (-1)
        DateTimeUtils.millisToDays(-DateTimeUtils.MILLIS_PER_DAY - 1, utc) should be (-2)
        DateTimeUtils.millisToDays(-DateTimeUtils.MILLIS_PER_DAY + 1, utc) should be (-1)
    }
}
