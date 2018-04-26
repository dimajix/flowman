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

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class ShortTypeTest extends FlatSpec with Matchers {
    "A ShortType" should "parse strings" in {
        ShortType.parse("12").asInstanceOf[Short] should be (12)
    }

    it should "support interpolation of SingleValues" in {
        ShortType.interpolate(SingleValue("12"), null).head.asInstanceOf[Short] should be (12)
    }

    it should "support interpolation of SingleValues with granularity" in {
        ShortType.interpolate(SingleValue("12"), "3").head.asInstanceOf[Short] should be (12)
        ShortType.interpolate(SingleValue("13"), "3").head.asInstanceOf[Short] should be (12)
        ShortType.interpolate(SingleValue("14"), "3").head.asInstanceOf[Short] should be (12)
        ShortType.interpolate(SingleValue("15"), "3").head.asInstanceOf[Short] should be (15)
    }

    it should "support interpolation of ArrayValues" in {
        val result = ShortType.interpolate(ArrayValue(Array("12","27")), null)
        result.head.asInstanceOf[Short] should be (12)
        result.drop(1).head.asInstanceOf[Short] should be (27)
    }

    it should "support interpolation of ArrayValues with granularity" in {
        val result = ShortType.interpolate(ArrayValue(Array("12","16","17")), "3").toSeq
        result(0).asInstanceOf[Short] should be (12)
        result(1).asInstanceOf[Short] should be (15)
        result(2).asInstanceOf[Short] should be (15)
    }

    it should "support interpolation of Ranges" in {
        val result = ShortType.interpolate(RangeValue("12","16"), null)
        result.map(_.asInstanceOf[Short]).toSeq should be (Seq(12,13,14,15).map(_.toShort))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = ShortType.interpolate(RangeValue("12","16"), "2")
        result.map(_.asInstanceOf[Short]).toSeq should be (Seq(12,14).map(_.toShort))

        val result2 = ShortType.interpolate(RangeValue("13","17"), "2")
        result2.map(_.asInstanceOf[Short]).toSeq should be (Seq(12,14).map(_.toShort))
    }
}
