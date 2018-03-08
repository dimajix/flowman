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
}
