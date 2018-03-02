package com.dimajix.flowman.spec.model

import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.MockedSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module


class HiveTableRelationTest extends FlatSpec with Matchers with MockedSparkSession  {
    "The HiveTableRelation" should "support create" in {
        val spec =
            """
              |relations:
              |  t0:
              |    type: table
              |    database: default
              |    table: lala
              |    schema:
              |      - name: str_col
              |        type: string
              |      - name: int_col
              |        type: integer
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        project.relations("t0").create(executor)
        verify(spark).sql(
            """CREATE  TABLE default.lala(
              |    str_col string,
              |    int_col int
              |)""".stripMargin)
    }

    it should "support single partition columns" in {
        val spec =
            """
              |relations:
              |  t0:
              |    type: table
              |    database: default
              |    table: lala
              |    schema:
              |      - name: str_col
              |        type: string
              |      - name: int_col
              |        type: integer
              |    partitions:
              |      - name: spart
              |        type: string
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        project.relations("t0").create(executor)
        verify(spark).sql(
            """CREATE  TABLE default.lala(
              |    str_col string,
              |    int_col int
              |)
              |PARTITIONED BY (spart string)""".stripMargin)
    }

    it should "support multiple partition columns" in {
        val spec =
            """
              |relations:
              |  t0:
              |    type: table
              |    database: default
              |    table: lala
              |    schema:
              |      - name: str_col
              |        type: string
              |      - name: int_col
              |        type: integer
              |    partitions:
              |      - name: spart
              |        type: string
              |      - name: ip
              |        type: int
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        project.relations("t0").create(executor)
        verify(spark).sql(
            """CREATE  TABLE default.lala(
              |    str_col string,
              |    int_col int
              |)
              |PARTITIONED BY (spart string, ip int)""".stripMargin)
    }
}
