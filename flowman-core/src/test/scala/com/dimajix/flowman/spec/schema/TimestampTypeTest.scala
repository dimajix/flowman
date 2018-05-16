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
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class TimestampTypeTest extends FlatSpec with Matchers {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.S]").withZone(ZoneOffset.UTC)
    def parseDateTime(value:String) = new Timestamp(LocalDateTime.parse(value, formatter).toEpochSecond(ZoneOffset.UTC) * 1000l)

    "A TimestampType" should "parse strings" in {
        TimestampType.parse("2017-12-01 12:21:20").asInstanceOf[Timestamp] should be (parseDateTime("2017-12-01 12:21:20"))
        TimestampType.parse("2017-12-01 12:21:20").asInstanceOf[Timestamp] should be (new Timestamp(1512130880*1000l))
        TimestampType.parse("2017-12-01 12:21:20.0").asInstanceOf[Timestamp] should be (parseDateTime("2017-12-01 12:21:20"))
        TimestampType.parse("2017-12-01 12:21:20.0").asInstanceOf[Timestamp] should be (new Timestamp(1512130880*1000l))
    }

    it should "be serialized as string identically" in {
        TimestampType.parse("2017-12-01 12:21:20").toString should be ("2017-12-01 12:21:20.0")
        TimestampType.parse("2017-12-01 12:00:00").toString should be ("2017-12-01 12:00:00.0")
    }

    it should "support interpolation of SingleValues" in {
        TimestampType.interpolate(SingleValue("2017-12-20 10:11:12"), null).head.asInstanceOf[Timestamp] should be (parseDateTime("2017-12-20 10:11:12"))
    }

    it should "support interpolation of SingleValues with granularity" in {
        TimestampType.interpolate(SingleValue("2017-12-20 10:11:12"), "PT1H").head.asInstanceOf[Timestamp] should be (parseDateTime("2017-12-20 10:00:00"))
        TimestampType.interpolate(SingleValue("2017-12-20 10:11:12"), "P1D").head.asInstanceOf[Timestamp] should be (parseDateTime("2017-12-20 00:00:00"))
    }

    it should "support interpolation of ArrayValues" in {
        val result = TimestampType.interpolate(ArrayValue(Array("2017-12-10 12:21:20","2017-12-10 12:21:40")), null).toSeq
        result.size should be (2)
        result(0).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10 12:21:20"))
        result(1).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10 12:21:40"))
    }

    it should "support interpolation of ArrayValues with granularity" in {
        val result = TimestampType.interpolate(ArrayValue(Array("2017-12-10 12:21:20","2017-12-10 12:21:40")), "PT1H").toSeq
        result.size should be (2)
        result(0).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10 12:00:00"))
        result(1).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10 12:00:00"))
    }

    it should "support interpolation of Ranges" in {
        val result = TimestampType.interpolate(RangeValue("2017-12-10 01:02:03","2017-12-10 01:02:07"), null).toSeq
        result.size should be (4)
        result(0).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10 01:02:03"))
        result(1).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10 01:02:04"))
        result(3).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10 01:02:06"))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = TimestampType.interpolate(RangeValue("2017-12-10 12:00:00","2017-12-18 00:00:00"), "P2D").toSeq
        result.size should be (4)
        result(0).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10 00:00:00"))
        result(1).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-12 00:00:00"))
        result(3).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-16 00:00:00"))

        val result2 = TimestampType.interpolate(RangeValue("2017-12-10 12:00:00","2017-12-18 01:00:00"), "P2D").toSeq
        result2.size should be (4)
        result2(0).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10 00:00:00"))
        result2(1).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-12 00:00:00"))
        result2(2).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-14 00:00:00"))
        result2(3).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-16 00:00:00"))

        val result3 = TimestampType.interpolate(RangeValue("2017-12-11 12:00:00","2017-12-19 01:00:00"), "P2D").toSeq
        result3.size should be (4)
        result3(0).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10 00:00:00"))
        result3(1).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-12 00:00:00"))
        result3(2).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-14 00:00:00"))
        result3(3).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-16 00:00:00"))
    }
}
