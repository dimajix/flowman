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

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class TimestampTypeTest extends FlatSpec with Matchers {
    def parseDateTime(value:String) = new Timestamp(LocalDateTime.parse(value).toEpochSecond(ZoneOffset.UTC)*1000l)

    "A TimestampType" should "parse strings" in {
        TimestampType.parse("2017-12-01T12:21:20").asInstanceOf[Timestamp] should be (parseDateTime("2017-12-01T12:21:20"))
        TimestampType.parse("2017-12-01T12:21:20").asInstanceOf[Timestamp] should be (new Timestamp(1512130880*1000l))
    }

    it should "support interpolation of SingleValues" in {
        TimestampType.interpolate(SingleValue("2017-12-20T10:11:12"), null).head.asInstanceOf[Timestamp] should be (parseDateTime("2017-12-20T10:11:12"))
    }

    it should "support interpolation of ArrayValues" in {
        val result = TimestampType.interpolate(ArrayValue(Array("2017-12-10T12:21:20","2017-12-10T12:21:40")), null).toSeq
        result.size should be (2)
        result(0).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10T12:21:20"))
        result(1).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10T12:21:40"))
    }

    it should "support interpolation of Ranges" in {
        val result = TimestampType.interpolate(RangeValue("2017-12-10T01:02:03","2017-12-10T01:02:07"), null).toSeq
        result.size should be (4)
        result(0).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10T01:02:03"))
        result(1).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10T01:02:04"))
        result(3).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10T01:02:06"))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = TimestampType.interpolate(RangeValue("2017-12-10T00:00:00","2017-12-18T00:00:00"), "P2D").toSeq
        result.size should be (4)
        result(0).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-10T00:00:00"))
        result(1).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-12T00:00:00"))
        result(3).asInstanceOf[Timestamp] should be (parseDateTime("2017-12-16T00:00:00"))
    }
}
