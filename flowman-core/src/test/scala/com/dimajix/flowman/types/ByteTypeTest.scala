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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.util.ObjectMapper


class ByteTypeTest extends AnyFlatSpec with Matchers {
    "A ByteType" should "be deserializable" in {
        ObjectMapper.parse[FieldType]("byte") should be(ByteType)
        ObjectMapper.parse[FieldType]("tinyint") should be(ByteType)
    }

    it should "parse strings" in {
        ByteType.parse("12") should be (12)
    }

    it should "support interpolation of SingleValues" in {
        ByteType.interpolate(SingleValue("12")).head should be (12)
    }

    it should "support interpolation of SingleValues with granularity" in {
        ByteType.interpolate(SingleValue("12"), Some("3")).head should be (12)
        ByteType.interpolate(SingleValue("13"), Some("3")).head should be (12)
        ByteType.interpolate(SingleValue("14"), Some("3")).head should be (12)
        ByteType.interpolate(SingleValue("15"), Some("3")).head should be (15)
    }

    it should "support interpolation of ArrayValues" in {
        val result = ByteType.interpolate(ArrayValue(Array("12","27")))
        result.head should be (12)
        result.drop(1).head should be (27)
    }

    it should "support interpolation of Ranges" in {
        val result = ByteType.interpolate(RangeValue("12","16"))
        result.toSeq should be (Seq(12,13,14,15).map(_.toByte))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = ByteType.interpolate(RangeValue("12","16"), Some("2"))
        result.toSeq should be (Seq(12,14).map(_.toByte))

        val result2 = ByteType.interpolate(RangeValue("13","17"), Some("2"))
        result2.toSeq should be (Seq(12,14).map(_.toByte))
    }

    it should "provide the correct Spark type" in {
        ByteType.sparkType should be (org.apache.spark.sql.types.ByteType)
    }

    it should "provide the correct SQL type" in {
        ByteType.sqlType should be ("tinyint")
        ByteType.typeName should be ("byte")
        ByteType.sparkType.sql should be ("TINYINT")
    }
}
