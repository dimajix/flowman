package com.dimajix.dataflow.spec

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.scalatest.FlatSpec
import org.scalatest.Matchers


class ValueOrRangeTest extends FlatSpec with Matchers {
    "A ValueOrRange" should "be readable from a single value" in {
        val spec =
            """
              |"someValue"
            """.stripMargin

        val mapper = new ObjectMapper(new YAMLFactory())
        val result = mapper.readValue(spec, classOf[ValueOrRange])
        result shouldBe a [SingleValue]
        result.asInstanceOf[SingleValue].value should be("someValue")
    }

    it should "be readable from an array value" in {
        val spec =
            """
              |["someValue", "secondValue"]
            """.stripMargin

        val mapper = new ObjectMapper(new YAMLFactory())
        val result = mapper.readValue(spec, classOf[ValueOrRange])
        result shouldBe a [ArrayValue]
        result.asInstanceOf[ArrayValue].value should be (Array("someValue","secondValue"))
    }

    it should "be readable from a range definition value" in {
        val spec =
            """
              |start: "someValue"
              |end: "secondValue"
            """.stripMargin

        val mapper = new ObjectMapper(new YAMLFactory())
        val result = mapper.readValue(spec, classOf[ValueOrRange])
        result shouldBe a [RangeValue]
        result.asInstanceOf[RangeValue].start should be ("someValue")
        result.asInstanceOf[RangeValue].end should be ("secondValue")
    }
}
