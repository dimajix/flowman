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

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class LongTypeTest extends FlatSpec with Matchers {
    "A LongType" should "parse strings" in {
        LongType.parse("12") should be (12)
    }

    it should "support interpolation of SingleValues" in {
        LongType.interpolate(SingleValue("12"), null).head should be (12)
    }

    it should "support interpolation of SingleValues with granularity" in {
        LongType.interpolate(SingleValue("12"), "3").head should be (12)
        LongType.interpolate(SingleValue("13"), "3").head should be (12)
        LongType.interpolate(SingleValue("14"), "3").head should be (12)
        LongType.interpolate(SingleValue("15"), "3").head should be (15)
    }

    it should "support interpolation of ArrayValues" in {
        val result = LongType.interpolate(ArrayValue(Array("12","27")), null)
        result.head should be (12)
        result.drop(1).head should be (27)
    }

    it should "support interpolation of ArrayValues with granularity" in {
        val result = LongType.interpolate(ArrayValue(Array("12","16","17")), "3").toSeq
        result(0) should be (12)
        result(1) should be (15)
        result(2) should be (15)
    }

    it should "support interpolation of Ranges" in {
        val result = LongType.interpolate(RangeValue("12","16"), null)
        result.toSeq should be (Seq(12,13,14,15).map(_.toLong))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = LongType.interpolate(RangeValue("12","16"), "2")
        result.toSeq should be (Seq(12,14).map(_.toLong))

        val result2 = LongType.interpolate(RangeValue("13","17"), "2")
        result2.toSeq should be (Seq(12,14).map(_.toLong))
    }

    it should "support interpolation of Ranges with steps" in {
        val result = LongType.interpolate(RangeValue("12","16", "2"), null)
        result.toSeq should be (Seq(12,14))

        val result2 = LongType.interpolate(RangeValue("13","17", "2"), null)
        result2.toSeq should be (Seq(13,15))
    }

    it should "support interpolation of Ranges with steps and granularity" in {
        val result = LongType.interpolate(RangeValue("12","16", "2"), "2")
        result.toSeq should be (Seq(12,14))

        val result1 = LongType.interpolate(RangeValue("13","17", "2"), "2")
        result1.toSeq should be (Seq(12,14))

        val result2 = LongType.interpolate(RangeValue("13","17", "3"), "2")
        result2.toSeq should be (Seq(12,16))
    }

    it should "provide the correct SQL type" in {
        val ftype = LongType
        ftype.sqlType should be ("long")
    }
}
