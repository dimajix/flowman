package com.dimajix.flowman.spec.flow

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.TableIdentifier


class InputMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    "The InputMapping" should "be able to read from a NullRelation" in {
        val spec =
            """
              |relations:
              |  empty:
              |    type: null
              |mappings:
              |  empty:
              |    type: read
              |    source: empty
              |    columns:
              |      str_col: string
              |      int_col: integer
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.relations.keys should contain("empty")
        project.mappings.keys should contain("empty")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.createExecutor(project)
        val df = executor.instantiate(TableIdentifier("empty"))
        df.columns should contain("str_col")
        df.columns should contain("int_col")
    }

    it should "support embedded schema" in {
        val spec =
            """
              |relations:
              |  empty:
              |    type: null
              |    schema:
              |      - name: str_col
              |        type: string
              |      - name: int_col
              |        type: integer
              |mappings:
              |  empty:
              |    type: read
              |    source: empty
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.relations.keys should contain("empty")
        project.mappings.keys should contain("empty")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.createExecutor(project)
        val df = executor.instantiate(TableIdentifier("empty"))
        df.columns should contain("str_col")
        df.columns should contain("int_col")
    }
}
