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

class JacksonTest extends FlatSpec with Matchers {
    "The BackReference" should "be filled out" in {
       val json =
           """
             |children:
             |   - name: lala
           """.stripMargin

        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        val data = mapper.readValue(json, classOf[SequenceContainer])

        data._children.size should be (1)
        data._children(0) should not be (null)
        data._children(0).name should be ("lala")
        data._children(0).parent should be (null)
    }
}
