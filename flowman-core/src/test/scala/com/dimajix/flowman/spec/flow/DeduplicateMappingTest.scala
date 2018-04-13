package com.dimajix.flowman.spec.flow

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.TableIdentifier


case class Record(c1:String, c2:String)


class DeduplicateMappingTest extends FlatSpec with Matchers with LocalSparkSession {

    "The DeduplicateMapping" should "work without list of columns" in {
        val sparkSession = spark
        import sparkSession.implicits._

        val spec =
            """
              |mappings:
              |  dummy:
              |    type: provided
              |    table: my_table
              |  dedup:
              |    type: deduplicate
              |    input: dummy
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.mappings.keys should contain("dedup")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.createExecutor(project)

        executor.spark.createDataFrame(Seq(
            Record("c1_v1", "c2_v1"),
            Record("c1_v1", "c2_v2"),
            Record("c1_v1", "c2_v2")
        )).createOrReplaceTempView("my_table")

        val df = executor.instantiate(TableIdentifier("dedup"))
        val rows = df.as[Record].collect()
        rows.size should be(2)
    }

}
