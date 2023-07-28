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

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.util.ObjectMapper


class TimestampTypeTest extends AnyFlatSpec with Matchers {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.S]").withZone(ZoneOffset.UTC)
    def parseDateTime(value:String) = new Timestamp(LocalDateTime.parse(value, formatter).toEpochSecond(ZoneOffset.UTC) * 1000l)

    "A TimestampType" should "be deserializable" in {
        ObjectMapper.parse[FieldType]("timestamp") should be(TimestampType)
    }

    it should "parseable from a SQL string" in {
        FieldType.of("timestamp") should be(TimestampType)
    }

    it should "parse strings" in {
        TimestampType.parse("2017-12-01T12:21:20").toTimestamp should be (parseDateTime("2017-12-01T12:21:20"))
        TimestampType.parse("2017-12-01T12:21:20Z").toTimestamp should be (parseDateTime("2017-12-01T12:21:20"))
        TimestampType.parse("2017-12-01T12:21:20+00").toTimestamp should be (parseDateTime("2017-12-01T12:21:20"))
        TimestampType.parse("2017-12-01T12:21:20+0000").toTimestamp should be (parseDateTime("2017-12-01T12:21:20"))
        TimestampType.parse("2017-12-01T12:21:20+01").toTimestamp should be (parseDateTime("2017-12-01T11:21:20"))
        TimestampType.parse("2017-12-01T12:21:20+0100").toTimestamp should be (parseDateTime("2017-12-01T11:21:20"))
        TimestampType.parse("2017-12-01T12:21:20").toTimestamp should be (new Timestamp(1512130880*1000l))
        TimestampType.parse("2017-12-01T12:21:20.0").toTimestamp should be (parseDateTime("2017-12-01T12:21:20"))
        TimestampType.parse("2017-12-01T12:21:20.0").toTimestamp should be (new Timestamp(1512130880*1000l))
    }

    it should "be serialized as string identically" in {
        TimestampType.parse("2017-12-01T12:21:20").toString should be ("2017-12-01T12:21:20.0")
        TimestampType.parse("2017-12-01T12:00:00").toString should be ("2017-12-01T12:00:00.0")
    }

    it should "support interpolation of SingleValues" in {
        TimestampType.interpolate(SingleValue("2017-12-20T10:11:12"), None).head.toTimestamp should be (parseDateTime("2017-12-20T10:11:12"))
    }

    it should "support interpolation of SingleValues with granularity" in {
        TimestampType.interpolate(SingleValue("2017-12-20T10:11:12"), Some("PT1H")).head.toTimestamp should be (parseDateTime("2017-12-20T10:00:00"))
        TimestampType.interpolate(SingleValue("2017-12-20T10:11:12"), Some("P1D")).head.toTimestamp should be (parseDateTime("2017-12-20T00:00:00"))
    }

    it should "support interpolation of ArrayValues" in {
        val result = TimestampType.interpolate(ArrayValue(Array("2017-12-10T12:21:20","2017-12-10T12:21:40")), None).toSeq
        result.size should be (2)
        result(0).toTimestamp should be (parseDateTime("2017-12-10T12:21:20"))
        result(1).toTimestamp should be (parseDateTime("2017-12-10T12:21:40"))
    }

    it should "support interpolation of ArrayValues with granularity" in {
        val result = TimestampType.interpolate(ArrayValue(Array("2017-12-10T12:21:20","2017-12-10T12:21:40")), Some("PT1H")).toSeq
        result.size should be (2)
        result(0).toTimestamp should be (parseDateTime("2017-12-10T12:00:00"))
        result(1).toTimestamp should be (parseDateTime("2017-12-10T12:00:00"))
    }

    it should "support interpolation of Ranges" in {
        val result = TimestampType.interpolate(RangeValue("2017-12-10T01:02:03","2017-12-10T01:02:07"), None).toSeq
        result.size should be (4)
        result(0).toTimestamp should be (parseDateTime("2017-12-10T01:02:03"))
        result(1).toTimestamp should be (parseDateTime("2017-12-10T01:02:04"))
        result(3).toTimestamp should be (parseDateTime("2017-12-10T01:02:06"))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = TimestampType.interpolate(RangeValue("2017-12-10T12:00:00","2017-12-18T00:00:00"), Some("P2D")).toSeq
        result.size should be (4)
        result(0).toTimestamp should be (parseDateTime("2017-12-10T00:00:00"))
        result(1).toTimestamp should be (parseDateTime("2017-12-12T00:00:00"))
        result(3).toTimestamp should be (parseDateTime("2017-12-16T00:00:00"))

        val result2 = TimestampType.interpolate(RangeValue("2017-12-10T12:00:00","2017-12-18T01:00:00"), Some("P2D")).toSeq
        result2.size should be (4)
        result2(0).toTimestamp should be (parseDateTime("2017-12-10T00:00:00"))
        result2(1).toTimestamp should be (parseDateTime("2017-12-12T00:00:00"))
        result2(2).toTimestamp should be (parseDateTime("2017-12-14T00:00:00"))
        result2(3).toTimestamp should be (parseDateTime("2017-12-16T00:00:00"))

        val result3 = TimestampType.interpolate(RangeValue("2017-12-11T12:00:00","2017-12-19T01:00:00"), Some("P2D")).toSeq
        result3.size should be (4)
        result3(0).toTimestamp should be (parseDateTime("2017-12-10T00:00:00"))
        result3(1).toTimestamp should be (parseDateTime("2017-12-12T00:00:00"))
        result3(2).toTimestamp should be (parseDateTime("2017-12-14T00:00:00"))
        result3(3).toTimestamp should be (parseDateTime("2017-12-16T00:00:00"))

        val result4 = TimestampType.interpolate(RangeValue("2017-12-11T00:00:00","2017-12-13T00:00:00"), Some("P1D")).toSeq
        result4.size should be (2)
        result4(0).toTimestamp should be (parseDateTime("2017-12-11T00:00:00"))
        result4(1).toTimestamp should be (parseDateTime("2017-12-12T00:00:00"))

        val result5 = TimestampType.interpolate(RangeValue("2017-12-11T00:00:00","2017-12-12T23:00:00"), Some("P1D")).toSeq
        result5.size should be (1)
        result5(0).toTimestamp should be (parseDateTime("2017-12-11T00:00:00"))
    }

    it should "support interpolation of Ranges with steps" in {
        val result = TimestampType.interpolate(RangeValue("2017-12-10T12:00:00","2017-12-18T00:00:00",Some("P2D"))).toSeq
        result.size should be (4)
        result(0).toTimestamp should be (parseDateTime("2017-12-10T12:00:00"))
        result(1).toTimestamp should be (parseDateTime("2017-12-12T12:00:00"))
        result(3).toTimestamp should be (parseDateTime("2017-12-16T12:00:00"))

        val result2 = TimestampType.interpolate(RangeValue("2017-12-10T12:00:00","2017-12-18T01:00:00", Some("P2D"))).toSeq
        result2.size should be (4)
        result2(0).toTimestamp should be (parseDateTime("2017-12-10T12:00:00"))
        result2(1).toTimestamp should be (parseDateTime("2017-12-12T12:00:00"))
        result2(2).toTimestamp should be (parseDateTime("2017-12-14T12:00:00"))
        result2(3).toTimestamp should be (parseDateTime("2017-12-16T12:00:00"))

        val result3 = TimestampType.interpolate(RangeValue("2017-12-11T12:00:00","2017-12-19T01:00:00", Some("P2D"))).toSeq
        result3.size should be (4)
        result3(0).toTimestamp should be (parseDateTime("2017-12-11T12:00:00"))
        result3(1).toTimestamp should be (parseDateTime("2017-12-13T12:00:00"))
        result3(2).toTimestamp should be (parseDateTime("2017-12-15T12:00:00"))
        result3(3).toTimestamp should be (parseDateTime("2017-12-17T12:00:00"))
    }

    it should "support interpolation of Ranges with steps and granularity" in {
        val result = TimestampType.interpolate(RangeValue("2017-12-10T12:00:00","2017-12-18T00:00:00",Some("P2D")), Some("P2D")).toSeq
        result.size should be (4)
        result(0).toTimestamp should be (parseDateTime("2017-12-10T00:00:00"))
        result(1).toTimestamp should be (parseDateTime("2017-12-12T00:00:00"))
        result(3).toTimestamp should be (parseDateTime("2017-12-16T00:00:00"))

        val result0 = TimestampType.interpolate(RangeValue("2017-12-10T12:00:00","2017-12-18T00:00:00",Some("P1D")), Some("P2D")).toSeq
        result0.size should be (4)
        result0(0).toTimestamp should be (parseDateTime("2017-12-10T00:00:00"))
        result0(1).toTimestamp should be (parseDateTime("2017-12-12T00:00:00"))
        result0(3).toTimestamp should be (parseDateTime("2017-12-16T00:00:00"))

        val result1 = TimestampType.interpolate(RangeValue("2017-12-10T12:00:00","2017-12-18T00:00:00",Some("P3D")), Some("P2D")).toSeq
        result1.size should be (3)
        result1(0).toTimestamp should be (parseDateTime("2017-12-10T00:00:00"))
        result1(1).toTimestamp should be (parseDateTime("2017-12-12T00:00:00"))
        result1(2).toTimestamp should be (parseDateTime("2017-12-16T00:00:00"))

        val result2 = TimestampType.interpolate(RangeValue("2017-12-10T12:00:00","2017-12-18T01:00:00", Some("P2D")), Some("P2D")).toSeq
        result2.size should be (4)
        result2(0).toTimestamp should be (parseDateTime("2017-12-10T00:00:00"))
        result2(1).toTimestamp should be (parseDateTime("2017-12-12T00:00:00"))
        result2(2).toTimestamp should be (parseDateTime("2017-12-14T00:00:00"))
        result2(3).toTimestamp should be (parseDateTime("2017-12-16T00:00:00"))

        val result3 = TimestampType.interpolate(RangeValue("2017-12-11T12:00:00","2017-12-19T01:00:00", Some("P2D")), Some("P2D")).toSeq
        result3.size should be (4)
        result3(0).toTimestamp should be (parseDateTime("2017-12-10T00:00:00"))
        result3(1).toTimestamp should be (parseDateTime("2017-12-12T00:00:00"))
        result3(2).toTimestamp should be (parseDateTime("2017-12-14T00:00:00"))
        result3(3).toTimestamp should be (parseDateTime("2017-12-16T00:00:00"))
    }

    it should "provide the correct Spark type" in {
        TimestampType.sparkType should be (org.apache.spark.sql.types.TimestampType)
    }

    it should "provide the correct SQL type" in {
        TimestampType.typeName should be ("timestamp")
        TimestampType.sqlType should be ("TIMESTAMP")
        TimestampType.sparkType.sql should be ("TIMESTAMP")
    }
}
