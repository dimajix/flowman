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

package com.dimajix.flowman.util

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class UtcTimestampTest extends AnyFlatSpec with Matchers {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm[:ss][.S]").withZone(ZoneOffset.UTC)
    def parseDateTime(value:String) = new Timestamp(LocalDateTime.parse(value, formatter).toEpochSecond(ZoneOffset.UTC) * 1000l)

    "A UtcTimestamp" should "parse strings" in {
        UtcTimestamp.parse("2017-12-01T12:21:20").toTimestamp() should be (parseDateTime("2017-12-01T12:21:20"))
        UtcTimestamp.parse("2017-12-01T12:21:20Z").toTimestamp() should be (parseDateTime("2017-12-01T12:21:20"))
        UtcTimestamp.parse("2017-12-01T12:21:20+00").toTimestamp() should be (parseDateTime("2017-12-01T12:21:20"))
        UtcTimestamp.parse("2017-12-01T12:21:20+0000").toTimestamp() should be (parseDateTime("2017-12-01T12:21:20"))
        UtcTimestamp.parse("2017-12-01T12:21:20+01").toTimestamp() should be (parseDateTime("2017-12-01T11:21:20"))
        UtcTimestamp.parse("2017-12-01T12:21:20+0100").toTimestamp() should be (parseDateTime("2017-12-01T11:21:20"))
        //UtcTimestamp.parse("2017-12-01T12:21:20+00:00").toTimestamp() should be (parseDateTime("2017-12-01T12:21:20"))
        UtcTimestamp.parse("2017-12-01T12:21:20").toTimestamp() should be (new Timestamp(1512130880*1000l))
        UtcTimestamp.parse("2017-12-01T12:21:20.0").toTimestamp() should be (parseDateTime("2017-12-01T12:21:20"))
        UtcTimestamp.parse("2017-12-01T12:21:20.0+00").toTimestamp() should be (parseDateTime("2017-12-01T12:21:20"))
        UtcTimestamp.parse("2017-12-01T12:21:20.0+0000").toTimestamp() should be (parseDateTime("2017-12-01T12:21:20"))
        UtcTimestamp.parse("2017-12-01T12:21:20.0+01").toTimestamp() should be (parseDateTime("2017-12-01T11:21:20"))
        UtcTimestamp.parse("2017-12-01T12:21:20.0+0100").toTimestamp() should be (parseDateTime("2017-12-01T11:21:20"))
        UtcTimestamp.parse("2017-12-01T12:21:20.0").toTimestamp() should be (new Timestamp(1512130880*1000l))
        UtcTimestamp.parse("2017-12-01T12:21").toTimestamp() should be (parseDateTime("2017-12-01T12:21"))
    }

    it should "be serialized as string identically" in {
        UtcTimestamp.parse("2017-12-01T12:21:20").toString should be ("2017-12-01T12:21:20.0")
    }

    it should "provide working accessors" in {
        val ts = UtcTimestamp.parse("2017-12-01T12:21:20")
        ts.getYear() should be (2017)
        ts.getMonth() should be (12)
        ts.getDayOfMonth() should be (1)
        ts.getHour() should be (12)
        ts.getMinute() should be (21)
        ts.getSecond() should be (20)
    }

    it should "support formatting" in {
        val ts = UtcTimestamp.parse("2017-12-01T12:21:20")
        ts.format("yyyyMMdd HHmmss") should be("20171201 122120")
        ts.format("'data/'yyyyMMdd'T'HHmmss") should be("data/20171201T122120")
    }
}
