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

package com.dimajix.flowman.spec.job

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.mockito.MockitoSugar

import com.dimajix.flowman.annotation.TargetType
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.metric.MetricSink
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.spec.target.BaseTarget
import com.dimajix.flowman.spec.target.Target
import com.dimajix.flowman.spec.target.TargetSpec
import com.dimajix.flowman.types.ArrayValue
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.RangeValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StringType


object GrabEnvironmentTarget {
    var environment:Map[String,Any] = Map()
}

case class GrabEnvironmentTarget(instanceProperties:Target.Properties) extends BaseTarget {
    /**
      * Abstract method which will perform the given task.
      *
      * @param executor
      */
    override def build(executor: Executor): Unit = {
        GrabEnvironmentTarget.environment = context.environment.filter{ case (k,v) => k != "project" }
    }
}

@TargetType(kind = "grabenv")
class GrabEnvironmentTargetSpec extends TargetSpec {
    override def instantiate(context: Context): GrabEnvironmentTarget = GrabEnvironmentTarget(instanceProperties(context))
}


class JobTest extends FlatSpec with Matchers with MockitoSugar {
    "A Job" should "be deseializable from" in {
        val spec =
            """
              |targets:
              |  grabenv:
              |    kind: grabenv
              |jobs:
              |  job:
              |    targets:
              |      - grabenv
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
              |targets:
              |  grabenv:
              |    kind: grabenv
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
              |      - name: p2
              |        default: v2
              |      - name: p3
              |        type: Integer
              |        default: 7
              |    targets:
              |      - grabenv
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withProject(project).build()
        val executor = session.executor

        val job = project.jobs("job").instantiate(session.getContext(project))
        job should not be (null)

        job.execute(executor, Phase.BUILD, Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        GrabEnvironmentTarget.environment should be (Map("p1" -> "v1", "p2" -> "v2", "p3" -> 7, "force" -> false))

        job.execute(executor, Phase.BUILD, Map("p1" -> "v1", "p2" -> "vx")) shouldBe (Status.SUCCESS)
        GrabEnvironmentTarget.environment should be (Map("p1" -> "v1", "p2" -> "vx", "p3" -> 7, "force" -> false))
    }

    it should "support overriding global parameters" in {
        val spec =
            """
              |environment:
              |  - p1=xxx
              |targets:
              |  grabenv:
              |    kind: grabenv
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
              |    targets:
              |      - grabenv
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withProject(project).build()
        val executor = session.executor

        val job = project.jobs("job").instantiate(session.getContext(project))
        job should not be (null)

        job.execute(executor, Phase.BUILD, Map("p1" -> "2"), false) shouldBe (Status.SUCCESS)
        GrabEnvironmentTarget.environment should be (Map("p1" -> "2", "force" -> false))
    }

    it should "support typed parameters" in {
        val spec =
            """
              |targets:
              |  grabenv:
              |    kind: grabenv
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
              |        type: Integer
              |    targets:
              |      - grabenv
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withProject(project).build()
        val executor = session.executor

        val job = project.jobs("job").instantiate(session.getContext(project))
        job should not be (null)

        job.execute(executor, Phase.BUILD, Map("p1" -> "2"), false) shouldBe (Status.SUCCESS)
        GrabEnvironmentTarget.environment should be (Map("p1" -> 2, "force" -> false))
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
        job.execute(executor, Phase.BUILD, Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        a[IllegalArgumentException] shouldBe thrownBy(job.execute(executor, Phase.BUILD, Map("p2" -> "v1")))
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
        job.execute(executor, Phase.BUILD, Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        a[IllegalArgumentException] shouldBe thrownBy(job.execute(executor, Phase.BUILD, Map("p2" -> "v1")))
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

        val job = module.jobs("job").instantiate(session.context)
        job should not be (null)
        job.execute(executor, Phase.BUILD, Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        a[IllegalArgumentException] shouldBe thrownBy(job.execute(executor, Phase.BUILD, Map()))
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

        val job = module.jobs("job").instantiate(session.context)
        job should not be (null)
        job.arguments(Map()) should be (Map("p2" -> "v2", "p3" -> 7))
        job.arguments(Map("p1" -> "lala")) should be (Map("p1" -> "lala", "p2" -> "v2", "p3" -> 7))
        job.arguments(Map("p2" -> "lala")) should be (Map("p2" -> "lala", "p3" -> 7))
    }

    it should "support environment" in {
        val spec =
            """
              |targets:
              |  grabenv:
              |    kind: grabenv
              |jobs:
              |  job:
              |    parameters:
              |      - name: p1
              |    environment:
              |      - p2=$p1
              |      - p3=xx${p2}yy
              |    targets:
              |      - grabenv
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withProject(project).build()
        val executor = session.executor

        val job = project.jobs("job").instantiate(session.getContext(project))
        job should not be (null)

        job.execute(executor, Phase.BUILD, Map("p1" -> "v1"), false) shouldBe (Status.SUCCESS)
        GrabEnvironmentTarget.environment should be (Map("p1" -> "v1", "p2" -> "v1", "p3" -> "xxv1yy", "force" -> false))
    }

    it should "support extending other jobs" in {
        val spec =
            """
              |targets:
              |  grabenv:
              |    kind: grabenv
              |  dummy:
              |    kind: "null"
              |
              |jobs:
              |  parent:
              |    parameters:
              |      - name: p2
              |    environment:
              |      - p3=abc
              |    targets:
              |      - dummy
              |  job:
              |    extends: parent
              |    parameters:
              |      - name: p1
              |    environment:
              |      - p2=$p1
              |      - p3=xx${p2}yy
              |    targets:
              |      - grabenv
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withProject(project).build()
        val executor = session.executor

        val job = project.jobs("job").instantiate(session.getContext(project))
        job should not be (null)
        job.targets.toSet should be (Set(TargetIdentifier("grabenv"), TargetIdentifier("dummy")))
        job.parameters should be (Seq(Job.Parameter("p1", StringType)))
        job.environment should be (Map("p2" -> "$p1", "p3" -> "xx${p2}yy"))

        job.execute(executor, Phase.BUILD, Map("p1" -> "v1"), false) shouldBe (Status.SUCCESS)
        GrabEnvironmentTarget.environment should be (Map("p1" -> "v1", "p2" -> "v1", "p3" -> "xxv1yy", "force" -> false))
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

        session.runner.executeJob(executor, job, Seq(Phase.BUILD), Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        verify(metricSink).addBoard(any())
        verify(metricSink).commit(any())
        verify(metricSink).removeBoard(any())
    }

    "Job.parseArguments" should "parse arguments" in {
        val session = Session.builder().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        val arguments = job.parseArguments(Map(
            "p1:start" -> "17",
            "p1:end" -> "27",
            "p2" -> "lala"
        ))

        arguments("p1") should be (RangeValue("17", "27"))
        arguments("p2") should be (SingleValue("lala"))
    }

    it should "throw an exception for unknown parameters" in {
        val session = Session.builder().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        an[IllegalArgumentException] should be thrownBy (job.parseArguments(Map(
            "p3" -> "lala"
        )))
    }

    "Job.interpolate" should "interpolate arguments" in {
        val session = Session.builder().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        val args = job.interpolate(Map(
            "p1"-> RangeValue("2", "8"),
            "p2" -> ArrayValue("x", "y", "z")
        ))

        args.toSet should be (Set(
            Map("p1" -> 2, "p2" -> "x"),
            Map("p1" -> 4, "p2" -> "x"),
            Map("p1" -> 6, "p2" -> "x"),
            Map("p1" -> 2, "p2" -> "y"),
            Map("p1" -> 4, "p2" -> "y"),
            Map("p1" -> 6, "p2" -> "y"),
            Map("p1" -> 2, "p2" -> "z"),
            Map("p1" -> 4, "p2" -> "z"),
            Map("p1" -> 6, "p2" -> "z")
        ))
    }

    it should "work with simple arguments" in {
        val session = Session.builder().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        val args = job.interpolate(Map(
            "p1"-> SingleValue("2"),
            "p2" -> SingleValue("x")
        ))

        args.toSet should be (Set(
            Map("p1" -> 2, "p2" -> "x")
        ))
    }

    it should "support granularity" in {
        val session = Session.builder().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        job.interpolate(Map(
            "p1"-> RangeValue("1", "7")
        )).toSet  should be (Set(
            Map("p1" -> 0),
            Map("p1" -> 2),
            Map("p1" -> 4)
        ))

        job.interpolate(Map(
            "p1"-> RangeValue("7", "7")
        )).toSet  should be (Set())

        job.interpolate(Map(
            "p1"-> RangeValue("7", "8")
        )).toSet  should be (Set(
            Map("p1" -> 6)
        ))
    }
}
