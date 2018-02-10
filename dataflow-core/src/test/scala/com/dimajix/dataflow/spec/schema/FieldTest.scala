package com.dimajix.dataflow.spec.schema

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.dataflow.execution.Session


class FieldTest extends FlatSpec with Matchers {
    lazy val mapper = {
        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        mapper
    }

    "A simple Field" should "be deserializable" in {
        val spec =
            """
              |name: lala
              |type: String
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.readValue(spec, classOf[Field])
        result.nullable should be (true)
        result.name should be ("lala")
        result.sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype should be (StringType)
    }

    "A struct Field" should "be deserializable" in {
        val spec =
            """
              |name: lala
              |type:
              |  type: struct
              |  fields:
              |    - name: lolo
              |      type: String
              |      nullable: false
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.readValue(spec, classOf[Field])
        result.nullable should be (true)
        result.name should be ("lala")
        result.sparkType shouldBe a[org.apache.spark.sql.types.StructType]
        result.sparkType should be (org.apache.spark.sql.types.StructType(
            org.apache.spark.sql.types.StructField("lolo", org.apache.spark.sql.types.StringType, false) :: Nil
        ))
        result.ftype shouldBe a[StructType]
        result.ftype.asInstanceOf[StructType].fields(0).name should be ("lolo")
        result.ftype.asInstanceOf[StructType].fields(0).ftype should be (StringType)
        result.ftype.asInstanceOf[StructType].fields(0).sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype.asInstanceOf[StructType].fields(0).nullable should be (false)
    }

    "An array Field" should "be deserializable" in {
        val spec =
            """
              |name: lala
              |type:
              |  type: array
              |  elementType: String
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.readValue(spec, classOf[Field])
        result.nullable should be (true)
        result.name should be ("lala")
        result.sparkType shouldBe a[org.apache.spark.sql.types.ArrayType]
        result.sparkType should be (org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.StringType))
    }
}
