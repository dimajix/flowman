/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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


class FieldValueTest extends AnyFlatSpec with Matchers {
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
