package com.dimajix.flowman.spec.flow

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.TableIdentifier


class ProvidedMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    "The ProvidedMapping" should "work" in {
        val spec =
            """
              |mappings:
              |  dummy:
              |    type: provided
              |    table: my_table
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.mappings.keys should contain("dummy")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.createExecutor(project)

        executor.spark.emptyDataFrame.createOrReplaceTempView("my_table")

        val df = executor.instantiate(TableIdentifier("dummy"))
        df.count should be(0)
    }

}
