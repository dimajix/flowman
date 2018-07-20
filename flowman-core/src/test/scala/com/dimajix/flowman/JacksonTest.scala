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

package com.dimajix.flowman

import com.fasterxml.jackson.annotation.JsonBackReference
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.FlatSpec
import org.scalatest.Matchers


class SequenceElement {
    @JsonBackReference
    var parent:SequenceContainer = _
    @JsonProperty(value="name")
    var name:String=_
}
class SequenceContainer {
    @JsonProperty(value="children") var _children: Seq[SequenceElement] = _
}

class OptionContainer {
    @JsonProperty(value="key") var key:String = _
    @JsonProperty(value="val") var value:Option[String] = _
}

class JacksonTest extends FlatSpec with Matchers {
    "The BackReference" should "be filled out" in {
       val yaml =
           """
             |children:
             |   - name: lala
           """.stripMargin

        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        val data = mapper.readValue(yaml, classOf[SequenceContainer])

        data._children.size should be (1)
        data._children(0) should not be (null)
        data._children(0).name should be ("lala")
        data._children(0).parent should be (null)
    }

    "Optional values" should "be supported" in {
        val yaml =
            """
              |key: some_key
              |val: some_value
            """.stripMargin

        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        val data = mapper.readValue(yaml, classOf[OptionContainer])
        data.key should be ("some_key")
        data.value should be (Some("some_value"))
    }

    it should "support missing values" in {
        val yaml =
            """
              |key: some_key
            """.stripMargin

        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        val data = mapper.readValue(yaml, classOf[OptionContainer])
        data.key should be ("some_key")
        data.value should be (null)
    }

    it should "support null values" in {
        val yaml =
            """
              |key: null
              |val: null
            """.stripMargin

        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        val data = mapper.readValue(yaml, classOf[OptionContainer])
        data.key should be (null)
        data.value should be (None)
    }

    it should "support null strings" in {
        val yaml =
            """
              |key: "null"
              |val: "null"
            """.stripMargin

        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        val data = mapper.readValue(yaml, classOf[OptionContainer])
        data.key should be ("null")
        data.value should be (Some("null"))
    }
}
