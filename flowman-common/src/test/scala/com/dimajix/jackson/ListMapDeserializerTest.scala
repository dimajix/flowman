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

package com.dimajix.jackson

import scala.collection.immutable.ListMap

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


object ListMapDeserializerTest {
    class SomeSpec {
        @JsonDeserialize(using = classOf[ListMapDeserializer])
        @JsonProperty(value = "columns", required = false) var columns:ListMap[String,String] = ListMap()
    }
}
class ListMapDeserializerTest extends AnyFlatSpec with Matchers {
    def mapper : ObjectMapper = {
        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
        mapper.registerModule(DefaultScalaModule)
        mapper
    }

    "The ListMapDeserializer" should "work" in {
        val spec =
            """
              |columns:
              |  _2: 2
              |  _3: 3
              |  _6: 6
              |  _5: 5
              |  _1: 1
              |  _4: 4
              |""".stripMargin
        val data = mapper.readValue(spec, classOf[ListMapDeserializerTest.SomeSpec])
        data.columns.toSeq should be (Seq(
            "_2" -> "2",
            "_3" -> "3",
            "_6" -> "6",
            "_5" -> "5",
            "_1" -> "1",
            "_4" -> "4"
        ))
    }
}
