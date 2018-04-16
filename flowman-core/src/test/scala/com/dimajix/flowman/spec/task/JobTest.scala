package com.dimajix.flowman.spec.task

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module


class JobTest extends FlatSpec with Matchers {
    "A Job" should "be deseializable from" in {
        val spec =
            """
              |jobs:
              |  job:
              |    tasks:
              |      - type: loop
              |        job: child
            """.stripMargin

        val module = Module.read.string(spec)
        val session = Session.builder().build()
        implicit val context = session.context

        val job = module.jobs("job")
        job should not be (null)
    }

    it should "support parameters" in {
        val spec =
            """
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
              |      - name: p2
              |        value: v2
              |      - name: p3
              |        type: Integer
              |        value: 7
            """.stripMargin

        val module = Module.read.string(spec)
        val session = Session.builder().build()
        val executor = session.executor
        implicit val context = session.context

        val job = module.jobs("job")
        job should not be (null)
        job.execute(executor, Map("p1" -> "v1")) shouldBe (JobStatus.SUCCESS)
        job.execute(executor, Map("p1" -> "v1", "p2" -> "v2")) shouldBe (JobStatus.SUCCESS)
    }

}
