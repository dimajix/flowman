package com.dimajix.flowman.spec.schema

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.FlatSpec
import org.scalatest.Matchers


class DecimalTypeTest extends FlatSpec with Matchers {
    lazy val mapper = {
        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        mapper
    }

    "A decimal type" should "be deserializable" in {
        val spec =
            """
              |decimal(10,4)
            """.stripMargin

        val result = mapper.readValue(spec, classOf[FieldType])
        result.asInstanceOf[DecimalType].precision should be (10)
        result.asInstanceOf[DecimalType].scale should be (4)
        result.sparkType should be (org.apache.spark.sql.types.DecimalType(10,4))
    }
}
