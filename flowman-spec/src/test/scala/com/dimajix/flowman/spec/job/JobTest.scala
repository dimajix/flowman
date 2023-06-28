/*
 * Copyright (C) 2018 The Flowman Authors
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

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.CyclePolicy
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.metric.MetricSink
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.JobWrapper
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.NamespaceWrapper
import com.dimajix.flowman.model.ProjectWrapper
import com.dimajix.flowman.model.SessionWrapper
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.spec.annotation.TargetType
import com.dimajix.flowman.spec.target.TargetSpec
import com.dimajix.flowman.types.StringType
import com.dimajix.spark.testing.LocalSparkSession


object GrabEnvironmentTarget {
    var environment:Map[String,Any] = Map()
}

case class GrabEnvironmentTarget(instanceProperties:Target.Properties) extends BaseTarget {
    /**
      * Abstract method which will perform the given task.
      *
      * @param executor
      */
    override def build(executor: Execution): Unit = {
        GrabEnvironmentTarget.environment = context.environment.toMap
    }
}

@TargetType(kind = "grabenv")
class GrabEnvironmentTargetSpec extends TargetSpec {
    override def instantiate(context: Context, properties:Option[Target.Properties]): GrabEnvironmentTarget = GrabEnvironmentTarget(instanceProperties(context, None))
}


class JobTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "A Job" should "be deseializable from" in {
        val spec =
            """
              |targets:
              |  grabenv:
              |    kind: grabenv
              |
              |jobs:
              |  job:
              |    description: Some Job
              |    targets:
              |      - grabenv
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val job = context.getJob(JobIdentifier("job"))
        job.name should be ("job")
        job.identifier should be (JobIdentifier("project/job"))
        job.description should be (Some("Some Job"))
        job.targets should be (Seq(TargetIdentifier("grabenv")))
        job.executions should be (Seq.empty)

        session.shutdown()
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
        val session = Session.builder()
            .withSparkSession(spark)
            .withProject(project)
            .build()
        val executor = session.execution
        val context = session.getContext(project)

        val job = project.jobs("job").instantiate(context)
        job should not be (null)

        job.execute(executor, Phase.BUILD, Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        GrabEnvironmentTarget.environment should be (Map(
            "job" -> JobWrapper(job),
            "project" -> ProjectWrapper(project),
            "session" -> SessionWrapper(session),
            "namespace" -> NamespaceWrapper(None),
            "p1" -> "v1",
            "p2" -> "v2",
            "p3" -> 7,
            "force" -> false,
            "dryRun" -> false)
        )

        job.execute(executor, Phase.BUILD, Map("p1" -> "v1", "p2" -> "vx")) shouldBe (Status.SUCCESS)
        GrabEnvironmentTarget.environment should be (Map(
            "job" -> JobWrapper(job),
            "project" -> ProjectWrapper(project),
            "session" -> SessionWrapper(session),
            "namespace" -> NamespaceWrapper(None),
            "p1" -> "v1",
            "p2" -> "vx",
            "p3" -> 7,
            "force" -> false,
            "dryRun" -> false)
        )

        session.shutdown()
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
        val session = Session.builder()
            .withSparkSession(spark)
            .withProject(project)
            .build()
        val executor = session.execution
        val context = session.getContext(project)

        val job = project.jobs("job").instantiate(context)
        job should not be (null)

        job.execute(executor, Phase.BUILD, Map("p1" -> "2"), force=false) shouldBe (Status.SUCCESS)
        GrabEnvironmentTarget.environment should be (Map(
            "job" -> JobWrapper(job),
            "project" -> ProjectWrapper(project),
            "session" -> SessionWrapper(session),
            "namespace" -> NamespaceWrapper(None),
            "p1" -> "2",
            "force" -> false,
            "dryRun" -> false)
        )

        session.shutdown()
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
        val session = Session.builder()
            .withSparkSession(spark)
            .withProject(project)
            .build()
        val executor = session.execution
        val context = session.getContext(project)

        val job = project.jobs("job").instantiate(context)
        job should not be (null)

        job.execute(executor, Phase.BUILD, Map("p1" -> "2"), force=false) shouldBe (Status.SUCCESS)
        GrabEnvironmentTarget.environment should be (Map(
            "job" -> JobWrapper(job),
            "project" -> ProjectWrapper(project),
            "session" -> SessionWrapper(session),
            "namespace" -> NamespaceWrapper(None),
            "p1" -> 2,
            "force" -> false,
            "dryRun" -> false)
        )

        session.shutdown()
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
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val job = module.jobs("job").instantiate(session.context)
        job should not be (null)
        job.execute(executor, Phase.BUILD, Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        a[IllegalArgumentException] shouldBe thrownBy(job.execute(executor, Phase.BUILD, Map("p2" -> "v1")))

        session.shutdown()
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
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val job = module.jobs("job").instantiate(session.context)
        job should not be (null)
        job.execute(executor, Phase.BUILD, Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        a[IllegalArgumentException] shouldBe thrownBy(job.execute(executor, Phase.BUILD, Map("p2" -> "v1")))

        session.shutdown()
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
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val job = module.jobs("job").instantiate(session.context)
        job should not be (null)
        job.execute(executor, Phase.BUILD, Map("p1" -> "v1")) shouldBe (Status.SUCCESS)
        a[IllegalArgumentException] shouldBe thrownBy(job.execute(executor, Phase.BUILD, Map()))

        session.shutdown()
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
        val session = Session.builder().withSparkSession(spark).build()

        val job = module.jobs("job").instantiate(session.context)
        job should not be (null)
        an[IllegalArgumentException] should be thrownBy(job.arguments(Map()))
        job.arguments(Map("p1" -> "lala")) should be (Map("p1" -> "lala", "p2" -> "v2", "p3" -> 7))
        job.arguments(Map("p1" -> "xyz", "p2" -> "lala")) should be (Map("p1" -> "xyz", "p2" -> "lala", "p3" -> 7))
        an[IllegalArgumentException] should be thrownBy(job.arguments(Map("p1" -> "xyz", "p4" -> "lala")))

        session.shutdown()
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
        val session = Session.builder()
            .withSparkSession(spark)
            .withProject(project)
            .build()
        val executor = session.execution
        val context = session.getContext(project)

        val job = project.jobs("job").instantiate(context)
        job should not be (null)

        job.execute(executor, Phase.BUILD, Map("p1" -> "v1"), force=false) shouldBe (Status.SUCCESS)
        GrabEnvironmentTarget.environment should be (Map(
            "job" -> JobWrapper(job),
            "project" -> ProjectWrapper(project),
            "session" -> SessionWrapper(session),
            "namespace" -> NamespaceWrapper(None),
            "p1" -> "v1",
            "p2" -> "v1",
            "p3" -> "xxv1yy",
            "force" -> false,
            "dryRun" -> false)
        )

        session.shutdown()
    }

    it should "support extending other jobs" in {
        val spec =
            """
              |targets:
              |  grabenv:
              |    kind: grabenv
              |  dummy:
              |    kind: empty
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
        val session = Session.builder()
            .withSparkSession(spark)
            .withProject(project)
            .build()
        val executor = session.execution
        val context = session.getContext(project)

        val job = project.jobs("job").instantiate(context)
        job should not be (null)
        job.targets.toSet should be (Set(TargetIdentifier("grabenv"), TargetIdentifier("dummy")))
        job.parameters should be (Seq(Job.Parameter("p1", StringType)))
        job.environment should be (Map("p2" -> "$p1", "p3" -> "xx${p2}yy"))

        job.execute(executor, Phase.BUILD, Map("p1" -> "v1"), force=false) shouldBe (Status.SUCCESS)
        GrabEnvironmentTarget.environment should be (Map(
            "job" -> JobWrapper(job),
            "project" -> ProjectWrapper(project),
            "session" -> SessionWrapper(session),
            "namespace" -> NamespaceWrapper(None),
            "p1" -> "v1",
            "p2" -> "v1",
            "p3" -> "xxv1yy",
            "force" -> false,
            "dryRun" -> false)
        )

        session.shutdown()
    }

    it should "support executions" in {
        val spec =
            """
              |jobs:
              |  main:
              |    executions:
              |      - phase: create
              |        cycle: first
              |      - phase: build
              |        cycle: always
              |        targets:
              |          - .*_daily
              |      - phase: build
              |        cycle: last
              |        targets:
              |          - .*_full
              |      - phase: verify
              |        cycle: last
              |        targets: documentation
              |""".stripMargin

        val project  = Module.read.string(spec).toProject("default")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val job = project.jobs("main").instantiate(context)
        job.executions.map(e => (e.phase, e.cycle, e.targets.map(_.toString()))) should be(Seq(
            (Phase.CREATE, CyclePolicy.FIRST, Seq(".*")),
            (Phase.BUILD, CyclePolicy.ALWAYS, Seq(".*_daily")),
            (Phase.BUILD, CyclePolicy.LAST, Seq(".*_full")),
            (Phase.VERIFY, CyclePolicy.LAST, Seq("documentation"))
        ))

        session.shutdown()
    }

    it should "support metrics" in {
        val spec =
            """
              |jobs:
              |  main:
              |    metadata:
              |      labels:
              |        job_label: xyz
              |    parameters:
              |      - name: p1
              |    environment:
              |      - p2=$p1
              |      - p3=xx${p2}yy
              |    metrics:
              |      labels:
              |        metric_label: abc
              |        job_label: $job
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
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.getContext(project)

        val metricSystem = executor.metricSystem
        val metricSink = stub[MetricSink]
        (metricSink.addBoard _).when(*,*).returns(Unit)
        (metricSink.commit _).when(*,*).returns(Unit)
        (metricSink.removeBoard _).when(*).returns(Unit)

        metricSystem.addSink(metricSink)

        val job = context.getJob(JobIdentifier("main"))
        job.metadata.labels should be (Map("job_label" -> "xyz"))

        session.runner.executeJob(job, Seq(Phase.BUILD), Map("p1" -> "v1")) shouldBe (Status.SUCCESS)

        session.shutdown()
    }
}
