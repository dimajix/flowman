package com.dimajix.jackson

import com.fasterxml.jackson.annotation.JsonBackReference
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


object JacksonTest {

    class SequenceElement {
        @JsonBackReference
        var parent: SequenceContainer = _
        @JsonProperty(value = "name")
        var name: String = _
    }

    class SequenceContainer {
        @JsonProperty(value = "children") var children: Seq[SequenceElement] = _
    }

    class OptionContainer {
        @JsonProperty(value = "key") var key: String = _
        @JsonProperty(value = "val") var value: Option[String] = _
    }

    class SpaceContainer {
        @JsonProperty(value = "some key") var some_key: String = _
        @JsonProperty(value = "map") var map: Map[String,String] = Map()
    }

    case class CaseClassWithDefaults (
        @JsonProperty(value = "key", defaultValue = "key") key: String = "key",
        @JsonProperty(value = "value", defaultValue = "value") value: String = "value"
    ) {
    }
}


class JacksonTest extends AnyFlatSpec with Matchers {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)

    import JacksonTest._

    "The BackReference" should "be filled out" in {
       val yaml =
           """
             |children:
             |   - name: lala
           """.stripMargin

        val data = mapper.readValue(yaml, classOf[SequenceContainer])

        data.children.size should be (1)
        data.children(0) should not be (null)
        data.children(0).name should be ("lala")
        data.children(0).parent should be (null)
    }

    "Case classes" should "be serialized with default values" in {
        val yaml =
            """
              |value: lala
            """.stripMargin

        val data = mapper.readValue(yaml, classOf[CaseClassWithDefaults])
        //data.key should be ("key")
        data.value should be ("lala")
    }

    "Spaces in keys" should "be supported" in {
        val yaml =
            """
              |some key: some_key
              |map:
              |  some key: some value
            """.stripMargin

        val data = mapper.readValue(yaml, classOf[SpaceContainer])
        data.some_key should be ("some_key")
        data.map should be (Map("some key" -> "some value"))
    }

    "Optional values" should "be supported" in {
        val yaml =
            """
              |key: some_key
              |val: some_value
            """.stripMargin

        val data = mapper.readValue(yaml, classOf[OptionContainer])
        data.key should be ("some_key")
        data.value should be (Some("some_value"))
    }

    it should "support missing values" in {
        val yaml =
            """
              |key: some_key
            """.stripMargin

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

        val data = mapper.readValue(yaml, classOf[OptionContainer])
        data.key should be ("null")
        data.value should be (Some("null"))
    }
}
