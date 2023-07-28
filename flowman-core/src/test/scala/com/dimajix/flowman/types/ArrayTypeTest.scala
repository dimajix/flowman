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


class ArrayTypeTest extends AnyFlatSpec with Matchers {
    "An ArrayType" should "be deserializable" in {
        val spec =
            """
              |  kind: array
              |  elementType: String
            """.stripMargin

        val result = ObjectMapper.parse[FieldType](spec)
        result shouldBe an[ArrayType]
        result.sparkType shouldBe a[org.apache.spark.sql.types.ArrayType]
        result.sparkType should be (org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.StringType, true))
    }
    it should "be deserializable with nullable elements" in {
        val spec =
            """
              |  kind: array
              |  containsNull: true
              |  elementType: String
            """.stripMargin

        val result = ObjectMapper.parse[FieldType](spec)
        result shouldBe an[ArrayType]
        result.sparkType shouldBe a[org.apache.spark.sql.types.ArrayType]
        result.sparkType should be (org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.StringType, true))
    }
    it should "be deserializable with non-nullable elements" in {
        val spec =
            """
              |  kind: array
              |  containsNull: false
              |  elementType: String
            """.stripMargin

        val result = ObjectMapper.parse[FieldType](spec)
        result shouldBe an[ArrayType]
        result.sparkType shouldBe a[org.apache.spark.sql.types.ArrayType]
        result.sparkType should be (org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.StringType, false))
    }
    it should "support nested Arrays" in {
        val spec =
            """
              |  kind: array
              |  containsNull: false
              |  elementType:
              |    kind: array
              |    elementType: String
            """.stripMargin

        val result = ObjectMapper.parse[FieldType](spec)
        result shouldBe an[ArrayType]
        result.sparkType shouldBe a[org.apache.spark.sql.types.ArrayType]
        result.sparkType should be (org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.StringType, true), false))
    }

    it should "provide the correct SQL type" in {
        val ftype = ArrayType(StringType, false)

        ftype.typeName should be("array")
        ftype.sqlType should be ("ARRAY<STRING>")
        ftype.sparkType.sql should be ("ARRAY<STRING>")
    }
}
