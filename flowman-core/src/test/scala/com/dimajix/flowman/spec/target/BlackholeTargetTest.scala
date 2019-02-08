package com.dimajix.flowman.spec.target

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Module


class BlackholeTargetTest extends FlatSpec with Matchers with LocalSparkSession{
    "A Blackhole Target" should "be writeable" in {
        val spark = this.spark

        val spec =
            s"""
               |targets:
               |  out:
               |    kind: blackhole
               |    input: some_table
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.getExecutor(project)
        implicit val context  = executor.context

        val output = project.targets("out")
        output.build(executor, Map(MappingIdentifier("some_table") -> spark.emptyDataFrame))
    }
}
