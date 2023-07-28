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


class MapTypeTest extends AnyFlatSpec with Matchers {
    "An MapType" should "be deserializable" in {
        val spec =
            """
              |  kind: map
              |  keyType: String
              |  valueType: String
            """.stripMargin

        val result = ObjectMapper.parse[FieldType](spec)
        result should be (MapType(StringType, StringType))
        result shouldBe an[MapType]
        result.sparkType shouldBe a[org.apache.spark.sql.types.MapType]
        result.sparkType should be (org.apache.spark.sql.types.MapType(org.apache.spark.sql.types.StringType, org.apache.spark.sql.types.StringType, true))
    }
    it should "be deserializable with nullable elements" in {
        val spec =
            """
              |  kind: map
              |  keyType: String
              |  valueType: String
              |  containsNull: true
            """.stripMargin

        val result = ObjectMapper.parse[FieldType](spec)
        result should be (MapType(StringType, StringType, true))
        result shouldBe an[MapType]
        result.sparkType shouldBe a[org.apache.spark.sql.types.MapType]
        result.sparkType should be (org.apache.spark.sql.types.MapType(org.apache.spark.sql.types.StringType, org.apache.spark.sql.types.StringType, true))
    }
    it should "be deserializable with non-nullable elements" in {
        val spec =
            """
              |  kind: map
              |  keyType: String
              |  valueType: String
              |  containsNull: false
            """.stripMargin

        val result = ObjectMapper.parse[FieldType](spec)
        result should be (MapType(StringType, StringType, false))
        result shouldBe an[MapType]
        result.sparkType shouldBe a[org.apache.spark.sql.types.MapType]
        result.sparkType should be (org.apache.spark.sql.types.MapType(org.apache.spark.sql.types.StringType, org.apache.spark.sql.types.StringType, false))
    }
    it should "support nested container types" in {
        val spec =
            """
              |  kind: map
              |  keyType: String
              |  containsNull: false
              |  valueType:
              |    kind: array
              |    elementType: String
            """.stripMargin

        val result = ObjectMapper.parse[FieldType](spec)
        result shouldBe an[MapType]
        result should be (MapType(StringType, ArrayType(StringType), false))
        result.sparkType shouldBe a[org.apache.spark.sql.types.MapType]
        result.sparkType should be (org.apache.spark.sql.types.MapType(org.apache.spark.sql.types.StringType, org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.StringType, true), false))
    }

    it should "provide the correct SQL type" in {
        val ftype = MapType(StringType, ArrayType(StringType), false)
        ftype.typeName should be("map")
        ftype.sqlType should be("MAP<STRING,ARRAY<STRING>>")
        ftype.sparkType.sql should be ("MAP<STRING, ARRAY<STRING>>")
    }
}
