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


class DoubleTypeTest  extends FlatSpec with Matchers {
    "A DoubleType" should "parse strings" in {
        DoubleType.parse("1.0") should be (1.0)
    }

    it should "support interpolation of SingleValues" in {
        DoubleType.interpolate(SingleValue("1.0"), None).head should be (1.0)
    }

    it should "support interpolation of ArrayValues" in {
        val result = DoubleType.interpolate(ArrayValue("1.0", "2.0"), None).toSeq
        result(0) should be (1.0)
        result(1) should be (2.0)
    }

    it should "support interpolation of Ranges" in {
        val result = DoubleType.interpolate(RangeValue("12","16"), None)
        result.toSeq should be (Seq(12.0,13.0,14.0,15.0))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = DoubleType.interpolate(RangeValue("12","16"), Some("2"))
        result.toSeq should be (Seq(12.0,14.0))

        val result2 = DoubleType.interpolate(RangeValue("13","17"), Some("2"))
        result2.toSeq should be (Seq(12.0,14.0))

        val result3 = DoubleType.interpolate(RangeValue("17","18"), Some("2"))
        result3.toSeq should be (Seq(16.0))
    }

    it should "support interpolation of Ranges with steps" in {
        val result = DoubleType.interpolate(RangeValue("12","16", Some("2")))
        result.toSeq should be (Seq(12.0,14.0))

        val result2 = DoubleType.interpolate(RangeValue("13","17", Some("2")))
        result2.toSeq should be (Seq(13.0,15.0))
    }

    it should "support interpolation of Ranges with steps and granularity" in {
        val result = DoubleType.interpolate(RangeValue("12","16", Some("2")), Some("2"))
        result.toSeq should be (Seq(12.0,14.0))

        val result1 = DoubleType.interpolate(RangeValue("13","17", Some("2")), Some("2"))
        result1.toSeq should be (Seq(12.0,14.0))

        val result2 = DoubleType.interpolate(RangeValue("13","17", Some("3")), Some("2"))
        result2.toSeq should be (Seq(12.0,16.0))
    }

    it should "provide the correct Spark type" in {
        DoubleType.sparkType should be (org.apache.spark.sql.types.DoubleType)
    }

    it should "provide the correct SQL type" in {
        DoubleType.sqlType should be ("double")
        DoubleType.sparkType.sql should be ("DOUBLE")
    }
}
