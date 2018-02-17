package com.dimajix.flowman.spec.task

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.OutputIdentifier


class OutputTaskTest extends FlatSpec with Matchers {
    "The OutputTask" should "support string assignments from code" in {
        val session = Session.builder().build()
        implicit val context = session.context
        val task = new OutputTask
        task.outputs = Seq("lala")
        task.outputs should equal(Seq(OutputIdentifier("lala",None)))
    }
    it should "support configuration via YML" in {
        val spec =
            """
              |jobs:
              |  dump:
              |    description: "Runs all outputs"
              |    tasks:
              |      - type: output
              |        outputs:
              |          - measurements
            """.stripMargin
        val module = Module.read.string(spec)
        val session = Session.builder().build()
        implicit val context = session.context
        module.jobs.size should be (1)
        val job = module.jobs("dump")
        job.tasks.size should be (1)
        val task = job.tasks(0).asInstanceOf[OutputTask]
        task.outputs.size should be (1)
    }
}
