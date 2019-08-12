/*
 * Copyright 2018 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.spec.task

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.ArgumentMatchers.any

import com.dimajix.flowman.annotation.TaskType
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.history.Status
import com.dimajix.flowman.metric.MetricBoard
import com.dimajix.flowman.metric.MetricSink
import com.dimajix.flowman.spec.JobIdentifier


object GrabEnvironmentTask {
    var environment:Map[String,Any] = Map()
}

case class GrabEnvironmentTask(instanceProperties:Task.Properties) extends BaseTask {
    /**
      * Abstract method which will perform the given task.
      *
      * @param executor
      */
    override def execute(executor: Executor): Boolean = {
        GrabEnvironmentTask.environment = context.environment
        true
    }
}

@TaskType(kind = "grabenv")
class GrabEnvironmentTaskSpec extends TaskSpec {
    override def instantiate(context: Context): GrabEnvironmentTask = GrabEnvironmentTask(instanceProperties(context))
}


class JobTest extends FlatSpec with Matchers with MockitoSugar {
    "A Job" should "be deseializable from" in {
        val spec =
            """
              |jobs:
              |  job:
              |    tasks:
              |      - kind: loop
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
              |        default: v2
              |      - name: p3
              |        type: Integer
              |        default: 7
              |    tasks:
              |      - kind: grabenv
            """.stripMargin

        val module = Module.read.string(spec)
        val session = Session.builder().build()
        val executor = session.executor

        val job = module.jobs("job").instantiate(session.context)
        job should not be (null)

        job.execute(executor, Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        GrabEnvironmentTask.environment should be (Map("p1" -> "v1", "p2" -> "v2", "p3" -> 7, "force" -> true))

        job.execute(executor, Map("p1" -> "v1", "p2" -> "vx")) shouldBe (Status.SUCCESS)
        GrabEnvironmentTask.environment should be (Map("p1" -> "v1", "p2" -> "vx", "p3" -> 7, "force" -> true))
    }

    it should "support overriding global parameters" in {
        val spec =
            """
              |environment:
              |  - p1=xxx
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
              |    tasks:
              |      - kind: grabenv
            """.stripMargin

        val module = Module.read.string(spec)
        val session = Session.builder().build()
        val executor = session.executor

        val job = module.jobs("job").instantiate(session.context)
        job should not be (null)

        job.execute(executor, Map("p1" -> "2")) shouldBe (Status.SUCCESS)
        GrabEnvironmentTask.environment should be (Map("p1" -> "2", "force" -> true))
    }

    it should "support typed parameters" in {
        val spec =
            """
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
              |        type: Integer
              |    tasks:
              |      - kind: grabenv
            """.stripMargin

        val module = Module.read.string(spec)
        val session = Session.builder().build()
        val executor = session.executor

        val job = module.jobs("job").instantiate(session.context)
        job should not be (null)

        job.execute(executor, Map("p1" -> "2")) shouldBe (Status.SUCCESS)
        GrabEnvironmentTask.environment should be (Map("p1" -> 2, "force" -> true))
    }

    it should "fail on undefined parameters" in {
        val spec =
            """
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
            """.stripMargin

        val module = Module.read.string(spec)
        val session = Session.builder().build()
        val executor = session.executor

        val job = module.jobs("job").instantiate(session.context)
        job should not be (null)
        job.execute(executor, Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        a[IllegalArgumentException] shouldBe thrownBy(job.execute(executor, Map("p2" -> "v1")))
    }

    it should "fail on undefined parameters, even if they are in the environment" in {
        val spec =
            """
              |environment:
              |  - p1=x
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
            """.stripMargin

        val module = Module.read.string(spec)
        val session = Session.builder().build()
        val executor = session.executor

        val job = module.jobs("job").instantiate(session.context)
        job should not be (null)
        job.execute(executor, Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        a[IllegalArgumentException] shouldBe thrownBy(job.execute(executor, Map("p2" -> "v1")))
    }

    it should "fail on missing parameters" in {
        val spec =
            """
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
            """.stripMargin

        val module = Module.read.string(spec)
        val session = Session.builder().build()
        val executor = session.executor
        implicit val context = session.context

        val job = module.jobs("job").instantiate(session.context)
        job should not be (null)
        job.execute(executor, Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        a[IllegalArgumentException] shouldBe thrownBy(job.execute(executor, Map()))
    }

    it should "return correct arguments" in {
        val spec =
            """
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
              |      - name: p2
              |        default: v2
              |      - name: p3
              |        type: Integer
              |        default: 7
            """.stripMargin

        val module = Module.read.string(spec)
        val session = Session.builder().build()
        val executor = session.executor

        val job = module.jobs("job").instantiate(session.context)
        job should not be (null)
        job.arguments(Map()) should be (Map("p1" -> null, "p2" -> "v2", "p3" -> 7))
        job.arguments(Map("p1" -> "lala")) should be (Map("p1" -> "lala", "p2" -> "v2", "p3" -> 7))
        job.arguments(Map("p2" -> "lala")) should be (Map("p1" -> null, "p2" -> "lala", "p3" -> 7))
    }

    it should "support environment" in {
        val spec =
            """
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
              |    environment:
              |      - p2=$p1
              |      - p3=xx${p2}yy
              |    tasks:
              |      - kind: grabenv
            """.stripMargin

        val module = Module.read.string(spec)
        val session = Session.builder().build()
        val executor = session.executor

        val job = module.jobs("job").instantiate(session.context)
        job should not be (null)

        job.execute(executor, Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        GrabEnvironmentTask.environment should be (Map("p1" -> "v1", "p2" -> "v1", "p3" -> "xxv1yy", "force" -> true))
    }

    it should "support metrics" in {
        val spec =
            """
              |jobs:
              |  main:
              |    labels:
              |      job_label: xyz
              |    parameters:
              |      - name: p1
              |    environment:
              |      - p2=$p1
              |      - p3=xx${p2}yy
              |    metrics:
              |      labels:
              |        metric_label: abc
              |      metrics:
              |        - name: metric_1
              |          labels:
              |            p1: $p1
              |            p2: $p2
              |            p3: $p3
              |          selector:
              |            name: job_runtime
              |            labels:
              |              name: main
              |    tasks:
              |      - kind: grabenv
            """.stripMargin

        val project  = Module.read.string(spec).toProject("default")
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.getContext(project)

        val metricSystem = executor.metrics
        val metricSink = mock[MetricSink]
        metricSystem.addSink(metricSink)

        val job = context.getJob(JobIdentifier("main"))
        job.labels should be (Map("job_label" -> "xyz"))

        job.execute(executor, Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        verify(metricSink).addBoard(any())
        verify(metricSink).commit(any())
        verify(metricSink).removeBoard(any())
    }
}
