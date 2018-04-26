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


class VarcharTypeTest extends FlatSpec with Matchers {
    lazy val mapper = {
        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        mapper
    }

    "A varchar type" should "be deserializable" in {
        val spec =
            """
              |varchar(14)
            """.stripMargin

        val result = mapper.readValue(spec, classOf[FieldType])
        result.asInstanceOf[VarcharType].length should be (14)
        result.sparkType should be (org.apache.spark.sql.types.VarcharType(14))
    }

    it should "parse strings" in {
        VarcharType(100).parse("lala").asInstanceOf[String] should be ("lala")
    }

    it should "support interpolation of SingleValues" in {
        VarcharType(100).interpolate(SingleValue("lala"), null).head.asInstanceOf[String] should be ("lala")
    }

    it should "support interpolation of ArrayValues" in {
        val result = VarcharType(100).interpolate(ArrayValue(Array("12","27")), null).toSeq
        result(0).asInstanceOf[String] should be ("12")
        result(1).asInstanceOf[String] should be ("27")
    }

}

