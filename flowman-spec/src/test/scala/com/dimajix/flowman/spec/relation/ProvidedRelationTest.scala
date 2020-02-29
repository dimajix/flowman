package com.dimajix.flowman.spec.relation

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.spark.testing.LocalSparkSession

case class Record(c1:String, c2:String)


class ProvidedRelationTest extends FlatSpec with Matchers with LocalSparkSession {
    "The ProvidedRelation" should "work" in {
        val spec =
            """
              |relations:
              |  dummy:
              |    kind: provided
              |    table: my_table
              |mappings:
              |  dummy:
              |    kind: read
              |    relation: dummy
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.relations.keys should contain("dummy")
        project.mappings.keys should contain("dummy")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        executor.spark.emptyDataFrame.createOrReplaceTempView("my_table")

        val mapping = context.getMapping(MappingIdentifier("dummy"))
        mapping should not be null

        val df = executor.instantiate(mapping, "main")
        df.count should be (0)
    }

}
