package com.dimajix.flowman.spec.schema

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session


class EmbeddedSchemaTest extends FlatSpec with Matchers {
    lazy val mapper = {
        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        mapper
    }

    "An EmbeddedSchema" should "be parseable with explicit type" in {
        val spec =
            """
              |type: embedded
              |fields:
              |  - name: str_col
              |    type: string
              |  - name: int_col
              |    type: integer
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.readValue(spec, classOf[Schema])
        result shouldBe a[EmbeddedSchema]
        result.fields.size should be (2)
        result.fields(context)(0).name should be ("str_col")
        result.fields(context)(1).name should be ("int_col")
    }

    it should "be parseable with inline type" in {
        val spec =
            """
              |type: inline
              |fields:
              |  - name: str_col
              |    type: string
              |  - name: int_col
              |    type: integer
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.readValue(spec, classOf[Schema])
        result shouldBe a[EmbeddedSchema]
        result.fields.size should be (2)
        result.fields(context)(0).name should be ("str_col")
        result.fields(context)(1).name should be ("int_col")
    }
}
