package com.dimajix.flowman.spec.output

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Module


class BlackholeOutputTest extends FlatSpec with Matchers with LocalSparkSession{
    "A Blackhole Output" should "be writeable" in {
        val spark = this.spark

        val spec =
            s"""
               |outputs:
               |  out:
               |    kind: blackhole
               |    input: some_table
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.getExecutor(project)
        implicit val context  = executor.context

        val output = project.outputs("out")
        output.execute(executor, Map(MappingIdentifier("some_table") -> spark.emptyDataFrame))
    }
}
