package com.dimajix.flowman.spec

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session

class ModuleTest extends FlatSpec with Matchers with LocalSparkSession {
    "The Module" should "be loadable from a string" in {
        val spec =
            """
              |environment:
              |  - x=y
              |config:
              |  - spark.lala=lolo
            """.stripMargin
        val module = Module.read.string(spec)
        module.environment should contain("x" -> "y")
        module.config should contain("spark.lala" -> "lolo")
    }

    it should "be executable" in {
        val spec =
            """
              |relations:
              |  empty:
              |    type: null
              |
              |outputs:
              |  blackhole:
              |    type: blackhole
              |    input: input
              |
              |mappings:
              |  input:
              |    type: read
              |    source: empty
              |    columns:
              |      col1: String
              |      col2: Integer
              |
              |jobs:
              |  default:
              |    tasks:
              |      - type: output
              |        outputs: blackhole
            """.stripMargin
        val project = Module.read.string(spec).toProject("default")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.createExecutor(project)
        val context = executor.context
        val runner = context.runner
        runner.execute(executor, project.jobs("default"))
    }
}
