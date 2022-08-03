/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.util.ObjectMapper


class IntegerTypeTest extends AnyFlatSpec with Matchers {
    "A IntegerType" should "be deserializable" in {
        ObjectMapper.parse[FieldType]("int") should be(IntegerType)
        ObjectMapper.parse[FieldType]("integer") should be(IntegerType)
    }

    it should "parse strings" in {
        IntegerType.parse("12") should be (12)
    }

    it should "support interpolation of SingleValues" in {
        IntegerType.interpolate(SingleValue("12"), None).head should be (12)
    }

    it should "support interpolation of SingleValues with granularity" in {
        IntegerType.interpolate(SingleValue("12"), Some("3")).head should be (12)
        IntegerType.interpolate(SingleValue("13"), Some("3")).head should be (12)
        IntegerType.interpolate(SingleValue("14"), Some("3")).head should be (12)
        IntegerType.interpolate(SingleValue("15"), Some("3")).head should be (15)
    }

    it should "support interpolation of ArrayValues" in {
        val result = IntegerType.interpolate(ArrayValue(Array("12","27")), None)
        result.head should be (12)
        result.drop(1).head should be (27)
    }

    it should "support interpolation of ArrayValues with granularity" in {
        val result = IntegerType.interpolate(ArrayValue(Array("12","16","17")), Some("3")).toSeq
        result(0) should be (12)
        result(1) should be (15)
        result(2) should be (15)
    }

    it should "support interpolation of Ranges" in {
        val result = IntegerType.interpolate(RangeValue("12","16"), None)
        result.toSeq should be (Seq(12,13,14,15))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = IntegerType.interpolate(RangeValue("12","16"), Some("2"))
        result.toSeq should be (Seq(12,14))

        val result2 = IntegerType.interpolate(RangeValue("13","17"), Some("2"))
        result2.toSeq should be (Seq(12,14))

        val result3 = IntegerType.interpolate(RangeValue("17","18"), Some("2"))
        result3.toSeq should be (Seq(16))
    }

    it should "support interpolation of Ranges with steps" in {
        val result = IntegerType.interpolate(RangeValue("12","16", Some("2")))
        result.toSeq should be (Seq(12,14))

        val result2 = IntegerType.interpolate(RangeValue("13","17", Some("2")))
        result2.toSeq should be (Seq(13,15))

        val result3 = IntegerType.interpolate(RangeValue("17","19", Some("2")))
        result3.toSeq should be (Seq(17))
    }

    it should "support interpolation of Ranges with steps and granularity" in {
        val result = IntegerType.interpolate(RangeValue("12","16", Some("2")), Some("2"))
        result.toSeq should be (Seq(12,14))

        val result1 = IntegerType.interpolate(RangeValue("13","17", Some("2")), Some("2"))
        result1.toSeq should be (Seq(12,14))

        val result2 = IntegerType.interpolate(RangeValue("13","17", Some("3")), Some("2"))
        result2.toSeq should be (Seq(12,16))

        val result3 = IntegerType.interpolate(RangeValue("17","21", Some("3")), Some("2"))
        result3.toSeq should be (Seq(16,20))
    }

    it should "provide the correct Spark type" in {
        IntegerType.sparkType should be (org.apache.spark.sql.types.IntegerType)
    }

    it should "provide the correct SQL type" in {
        IntegerType.sqlType should be ("int")
        IntegerType.sparkType.sql should be ("INT")
        IntegerType.typeName should be ("integer")
    }
}
