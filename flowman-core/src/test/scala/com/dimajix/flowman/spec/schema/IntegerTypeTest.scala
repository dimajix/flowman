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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.FlatSpec
import org.scalatest.Matchers


class IntegerTypeTest extends FlatSpec with Matchers {
    lazy val mapper = {
        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        mapper
    }

    "A IntegerType" should "parse strings" in {
        IntegerType.parse("12").asInstanceOf[Int] should be (12)
    }

    it should "support interpolation of SingleValues" in {
        IntegerType.interpolate(SingleValue("12"), null).head.asInstanceOf[Int] should be (12)
    }

    it should "support interpolation of ArrayValues" in {
        val result = IntegerType.interpolate(ArrayValue(Array("12","27")), null)
        result.head.asInstanceOf[Int] should be (12)
        result.drop(1).head.asInstanceOf[Int] should be (27)
    }

    it should "support interpolation of Ranges" in {
        val result = IntegerType.interpolate(RangeValue("12","16"), null)
        result.map(_.asInstanceOf[Int]).toSeq should be (Seq(12,13,14,15))
    }

    it should "support interpolation of Ranges with granularity" in {
        val result = IntegerType.interpolate(RangeValue("12","16"), "2")
        result.map(_.asInstanceOf[Int]).toSeq should be (Seq(12,14))
    }

    "A int type" should "be deserializable" in {
        val spec = "int"

        val result = mapper.readValue(spec, classOf[FieldType])
        result should be (IntegerType)
        result.sparkType should be (org.apache.spark.sql.types.IntegerType)
    }
    it should "be deserializable in long form" in {
        val spec = "integer"

        val result = mapper.readValue(spec, classOf[FieldType])
        result should be (IntegerType)
        result.sparkType should be (org.apache.spark.sql.types.IntegerType)
    }
}
