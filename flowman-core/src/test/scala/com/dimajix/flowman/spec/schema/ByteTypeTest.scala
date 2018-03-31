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


class ByteTypeTest extends FlatSpec with Matchers {
    "A ByteType" should "parse strings" in {
        ByteType.parse("12").asInstanceOf[Byte] should be (12)
    }

    it should "support interpolation of SingleValues" in {
        ByteType.interpolate(SingleValue("12"), null).head.asInstanceOf[Byte] should be (12)
    }

    it should "support interpolation of ArrayValues" in {
        val result = ByteType.interpolate(ArrayValue(Array("12","27")), null)
        result.head.asInstanceOf[Byte] should be (12)
        result.drop(1).head.asInstanceOf[Byte] should be (27)
    }

    it should "support interpolation of Ranges" in {
        val result = ByteType.interpolate(RangeValue("12","16"), null)
        result.map(_.asInstanceOf[Byte]).toSeq should be (Seq(12,13,14,15).map(_.toByte))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = ByteType.interpolate(RangeValue("12","16"), "2")
        result.map(_.asInstanceOf[Byte]).toSeq should be (Seq(12,14).map(_.toByte))
    }
}
