package com.dimajix.flowman.spec.task

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module


class LoopTaskTest extends FlatSpec with Matchers {
    "The LoopTask" should "support simple loops" in {
        val spec =
            """
              |jobs:
              |  child:
              |    parameters:
              |      - name: p1
              |      - name: p2
              |        value: v2
              |      - name: p3
              |        type: Integer
              |        value: 7
              |  loop:
              |    tasks:
              |      - type: loop
              |        job: child
              |        args:
              |          p1: xyz
              |          p2: [abc, xyz]
              |          p3:
              |            start: 1
              |            end: 3
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().build()
        val executor = session.createExecutor(project)
        implicit val context = session.context

        val job = project.jobs("loop")
        job.execute(executor, Map()) shouldBe (JobStatus.SUCCESS)
    }

    it should "fail on undefined parameters" in {
        val spec =
            """
              |jobs:
              |  child:
              |    parameters:
              |      - name: p1
              |  loop:
              |    tasks:
              |      - type: loop
              |        job: child
              |        args:
              |          p2: [abc, xyz]
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().build()
        val executor = session.createExecutor(project)
        implicit val context = session.context

        val job = project.jobs("loop")
        job.execute(executor, Map()) shouldBe (JobStatus.FAILURE)
    }
}
