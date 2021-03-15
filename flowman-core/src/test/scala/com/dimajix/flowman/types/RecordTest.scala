/*
 * Copyright 2021 Kaya Kupferschmidt
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

import com.dimajix.flowman.types.RecordTest.RecordListWrapper
import com.dimajix.flowman.util.ObjectMapper


object RecordTest {
    class RecordListWrapper {
        var records:Seq[Record] = Seq()
    }
}

class RecordTest extends AnyFlatSpec with Matchers {
    "A Record" should "be deserializable from a single numeric value" in {
        val spec =
            """
              |3
            """.stripMargin

        val result = ObjectMapper.parse[Record](spec)
        result should be (ValueRecord("3"))
    }
    it should "be deserializable from a single boolean value" in {
        val spec =
            """
              |true
            """.stripMargin

        val result = ObjectMapper.parse[Record](spec)
        result should be (ValueRecord("true"))
    }
    it should "not be deserializable from a single null value" in {
        val spec =
            """
              |null
            """.stripMargin

        val result = ObjectMapper.parse[Record](spec)
        result should be (null)
    }
    it should "be deserializable from a single string value" in {
        val spec =
            """
              |"text"
            """.stripMargin

        val result = ObjectMapper.parse[Record](spec)
        result should be (ValueRecord("text"))
    }

    it should "be deserializable from a map" in {
        val spec =
            """
              |field_1: lala
              |field_2: null
              |field_3: true
              |field_4: 12
              |""".stripMargin

        val result = ObjectMapper.parse[Record](spec)
        result should be (MapRecord(Map("field_1" -> "lala", "field_2" -> null, "field_3" -> "true", "field_4" -> "12")))

    }

    it should "be deserializable from an array" in {
        val spec =
            """
              |[1,"text",null,"",true]
            """.stripMargin

        val result = ObjectMapper.parse[Record](spec)
        result should be (ArrayRecord(Seq("1","text",null,"","true")))
    }
    it should "be deserializable from an empty array" in {
        val spec =
            """
              |[]
            """.stripMargin

        val result = ObjectMapper.parse[Record](spec)
        result should be (ArrayRecord(Seq()))
    }

    "An embedded list of Records" should "be deserializable from an  list" in {
        val spec =
            """
              |records: []
            """.stripMargin

        val result = ObjectMapper.parse[RecordListWrapper](spec)
        result.records should be (Seq())
    }
    it should "be deserializable from an single value" in {
        val spec =
            """
              |records: 1
            """.stripMargin

        val result = ObjectMapper.parse[RecordListWrapper](spec)
        result.records should be (Seq(ValueRecord("1")))
    }
    it should "not be deserializable from a null value" in {
        val spec =
            """
              |records: null
            """.stripMargin

        val result = ObjectMapper.parse[RecordListWrapper](spec)
        result.records should be (null)
    }

    it should "be deserializable from an singleton list with a value" in {
        val spec =
            """
              |records:
              | - 1
            """.stripMargin

        val result = ObjectMapper.parse[RecordListWrapper](spec)
        result.records should be (Seq(ValueRecord("1")))
    }
    it should "not be deserializable from a singleton list with a null value" in {
        val spec =
            """
              |records:
              | - null
            """.stripMargin

        val result = ObjectMapper.parse[RecordListWrapper](spec)
        result.records should be (Seq(null))
    }

    it should "be deserializable from an empty nested list" in {
        val spec =
            """
              |records:
              | - []
            """.stripMargin

        val result = ObjectMapper.parse[RecordListWrapper](spec)
        result.records should be (Seq(ArrayRecord(Seq())))
    }
    it should "be deserializable from an nested list with a single null value" in {
        val spec =
            """
              |records:
              | - [null]
            """.stripMargin

        val result = ObjectMapper.parse[RecordListWrapper](spec)
        result.records should be (Seq(ArrayRecord(Seq(null))))
    }
    it should "be deserializable from an nested list" in {
        val spec =
            """
              |records:
              | - [1,2,"text",null]
            """.stripMargin

        val result = ObjectMapper.parse[RecordListWrapper](spec)
        result.records should be (Seq(ArrayRecord(Seq("1","2","text",null))))
    }

    "An embedded list of maps" should "be deseriazable" in {
        val spec =
            """
              |records:
              | - field_1: lala
              |   field_2: null
              |   field_3: true
              |   field_4: 12
              |""".stripMargin

        val result = ObjectMapper.parse[RecordListWrapper](spec)
        result.records should be (Seq(MapRecord(Map("field_1" -> "lala", "field_2" -> null, "field_3" -> "true", "field_4" -> "12"))))
    }

    "A ValueRecord" should "return its value" in {
        val schema = StructType(Seq(
            Field("string", StringType)
        ))
        val record = ValueRecord("2")

        record.toArray(schema).toSeq should be (Seq("2"))
    }
    it should "fill up default values" in {
        val schema = StructType(Seq(
            Field("c1", StringType),
            Field("c2", IntegerType, default = Some("12")),
            Field("c3", StringType)
        ))
        val record = ValueRecord("2")

        record.toArray(schema).toSeq should be (Seq("2", "12", null))
    }

    "An ArrayRecord" should "return its values" in {
        val schema = StructType(Seq(
            Field("c1", StringType),
            Field("c2", StringType)
        ))
        val record = ArrayRecord(Seq("2","str"))

        record.toArray(schema).toSeq should be (Seq("2","str"))
    }
    it should "fill up default values" in {
        val schema = StructType(Seq(
            Field("c1", StringType),
            Field("c2", StringType),
            Field("c3", IntegerType, default = Some("12")),
            Field("c4", StringType)
        ))
        val record = ArrayRecord("2",null)

        record.toArray(schema).toSeq should be (Seq("2", null, "12", null))
    }
    it should "drop additonal values" in {
        val schema = StructType(Seq(
            Field("c1", StringType),
            Field("c2", IntegerType)
        ))
        val record = ArrayRecord("2","3","4")

        record.toArray(schema).toSeq should be (Seq("2", "3"))
    }

    "A MapRecord" should "return correct values" in {
        val schema = StructType(Seq(
            Field("c1", StringType),
            Field("c2", StringType),
            Field("c3", IntegerType, default = Some("12")),
            Field("c4", StringType)
        ))
        val record = MapRecord("c1" -> "2", "c2" -> null, "c5" -> "xxx")

        record.toArray(schema).toSeq should be (Seq("2", null, "12", null))
    }
}
