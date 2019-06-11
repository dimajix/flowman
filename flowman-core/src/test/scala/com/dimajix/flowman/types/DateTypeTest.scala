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

package com.dimajix.flowman.types

import java.sql.Date

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class DateTypeTest extends FlatSpec with Matchers {
    "A DateType" should "parse strings" in {
        DateType.parse("2017-12-01") should be (Date.valueOf("2017-12-01"))
    }

    it should "support interpolation of SingleValues" in {
        DateType.interpolate(SingleValue("2017-12-20")).head should be (Date.valueOf("2017-12-20"))
    }

    it should "support interpolation of SingleValues with granularity" in {
        DateType.interpolate(SingleValue("2017-12-20"), Some("P5D")).head should be (Date.valueOf("2017-12-20"))
        DateType.interpolate(SingleValue("2017-12-21"), Some("P5D")).head should be (Date.valueOf("2017-12-20"))
        DateType.interpolate(SingleValue("2017-12-22"), Some("P5D")).head should be (Date.valueOf("2017-12-20"))
        DateType.interpolate(SingleValue("2017-12-23"), Some("P5D")).head should be (Date.valueOf("2017-12-20"))
        DateType.interpolate(SingleValue("2017-12-24"), Some("P5D")).head should be (Date.valueOf("2017-12-20"))
        DateType.interpolate(SingleValue("2017-12-25"), Some("P5D")).head should be (Date.valueOf("2017-12-25"))
    }

    it should "support interpolation of ArrayValues" in {
        val result = DateType.interpolate(ArrayValue(Array("2017-12-10","2017-12-14"))).toSeq
        result.size should be (2)
        result(0) should be (Date.valueOf("2017-12-10"))
        result(1) should be (Date.valueOf("2017-12-14"))
    }

    it should "support interpolation of ArrayValues with granularity" in {
        val result = DateType.interpolate(ArrayValue(Array("2017-12-10","2017-12-11","2017-12-12")), Some("P3D")).toSeq
        result.size should be (3)
        result(0) should be (Date.valueOf("2017-12-08"))
        result(1) should be (Date.valueOf("2017-12-11"))
        result(2) should be (Date.valueOf("2017-12-11"))
    }

    it should "support interpolation of Ranges" in {
        val result = DateType.interpolate(RangeValue("2017-12-10","2017-12-14"), None).toSeq
        result.size should be (4)
        result(0) should be (Date.valueOf("2017-12-10"))
        result(1) should be (Date.valueOf("2017-12-11"))
        result(3) should be (Date.valueOf("2017-12-13"))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = DateType.interpolate(RangeValue("2017-12-10","2017-12-18"), Some("P2D")).toSeq
        result.size should be (4)
        result(0) should be (Date.valueOf("2017-12-10"))
        result(1) should be (Date.valueOf("2017-12-12"))
        result(3) should be (Date.valueOf("2017-12-16"))

        val result2 = DateType.interpolate(RangeValue("2017-12-11","2017-12-19"), Some("P2D")).toSeq
        result2.size should be (4)
        result2(0) should be (Date.valueOf("2017-12-10"))
        result2(1) should be (Date.valueOf("2017-12-12"))
        result2(3) should be (Date.valueOf("2017-12-16"))
    }

    it should "provide the correct SQL type" in {
        val ftype = DateType

        ftype.sqlType should be ("date")
    }
}
