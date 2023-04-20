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

import org.apache.spark.sql.functions.lit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.util.ObjectMapper
import com.dimajix.flowman.util.UtcTimestamp


class FieldValueTest extends AnyFlatSpec with Matchers {
    "FieldValue.asString" should "work" in {
        FieldValue.asString("abc") should be ("abc")
        FieldValue.asString(12) should be ("12")
        val ts = UtcTimestamp.parse("2023-03-10T10:22:10")
        FieldValue.asString(ts) should be ("2023-03-10 10:22:10")
        FieldValue.asString(ts.toTimestamp()) should be ("2023-03-10 10:22:10")
    }

    "FieldValue.asLiteral" should "work" in {
        FieldValue.asLiteral("abc") should be(lit("abc"))
        FieldValue.asLiteral(12) should be(lit(12))
        val ts = UtcTimestamp.parse("2023-03-10T10:22:10")
        FieldValue.asLiteral(ts) should be(lit(ts.toTimestamp()))
        FieldValue.asLiteral(ts.toTimestamp()) should be(lit(ts.toTimestamp()))
    }

    "A String" should "be deserializable as a SingleValue" in {
        val spec ="some_string"
        val value = ObjectMapper.parse[FieldValue](spec)
        value should be (SingleValue("some_string"))
    }

    "An Array" should "be deserializable as a ArrayValue" in {
        val spec =
            """
              |[entry_1,entry_2]
            """.stripMargin
        val value = ObjectMapper.parse[FieldValue](spec)
        value should be (ArrayValue("entry_1", "entry_2"))
    }

    it should "be deserializable as a ArrayValue from an extended notation" in {
        val spec =
            """
              |- entry_1
              |- entry_2
            """.stripMargin
        val value = ObjectMapper.parse[FieldValue](spec)
        value should be (ArrayValue("entry_1", "entry_2"))
    }

    "A Range" should "be deserializable as a RangeValue" in {
        val spec =
            """
              |start: 12
              |end: 24
              |step: 2
            """.stripMargin
        val value = ObjectMapper.parse[FieldValue](spec)
        value should be (RangeValue("12", "24", Some("2")))
    }

    it should "be deserializable as a RangeValue without a step" in {
        val spec =
            """
              |start: 12
              |end: 24
            """.stripMargin
        val value = ObjectMapper.parse[FieldValue](spec)
        value should be (RangeValue("12", "24"))
    }
}
