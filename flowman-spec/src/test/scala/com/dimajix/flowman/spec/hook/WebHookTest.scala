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

package com.dimajix.flowman.spec.hook

import java.time.Instant

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spec.target.EmptyTarget
import com.dimajix.flowman.spec.target.EmptyTargetSpec
import com.dimajix.flowman.types.StringType
import com.dimajix.spark.testing.LocalSparkSession


class WebHookTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The WebHook" should "provide a working job API" in {
        val session = Session.builder()
            .withEnvironment("env", "some_environment")
            .disableSpark()
            .build()
        val hook = WebHook(
            Hook.Properties(session.context),
            jobStart = Some("http://0.0.0.0/$env/$job/$arg1"),
            jobFinish = Some("http://0.0.0.0/$env/$job/$arg1/$status")
        )

        val job = Job.builder(session.context).build()
        val instance = JobDigest("default", "p1", "j1", Phase.BUILD, Map("arg1" -> "v1"))
        val execution = session.execution

        val token = hook.startJob(execution, job, instance, None)
        hook.finishJob(execution, token, JobResult(job, instance, Status.SUCCESS, Instant.now()))

        session.shutdown()
    }

    it should "provide a working target API" in {
        val session = Session.builder()
            .withEnvironment("env", "some_environment")
            .disableSpark()
            .build()
        val hook = new WebHook(
            Hook.Properties(session.context),
            targetStart = Some("http://0.0.0.0/$env/$target/$arg1"),
            targetFinish = Some("http://0.0.0.0/$env/$target/$arg1/$status")
        )

        val target = EmptyTarget(Target.Properties(session.context, "t1"), Map())
        val instance = TargetDigest("default", "p1", "t1", Phase.BUILD, Map("arg1" -> "v1"))
        val execution = session.execution

        val token = hook.startTarget(execution, target, instance, None)
        hook.finishTarget(execution, token, TargetResult(target, instance, Seq(), Status.SUCCESS, None, Instant.now(), Instant.now()))

        session.shutdown()
    }

    it should "be deserializable in a namespace" in {
        val spec =
            """
              |hooks:
              |  - kind: web
              |    jobStart: job_start/$job/$target
              |    jobFinish: job_finish/$job/$target
              |    jobSuccess: job_success/$job/$target
              |    jobSkip: job_skip/$job/$target
              |    jobFailure: job_failure/$job/$target
              |    targetStart: target_start/$job/$target
              |    targetFinish: target_finish/$job/$target
              |    targetSuccess: target_success/$job/$target
              |    targetSkip: target_skip/$job/$target
              |    targetFailure: target_failure/$job/$target
              |""".stripMargin
        val ns = Namespace.read.string(spec)
        val session = Session.builder()
                .withNamespace(ns)
                .disableSpark()
                .build()
        val hook = session.hooks.head.instantiate(session.context).asInstanceOf[WebHook]
        hook.jobStart should be (Some("job_start/$job/$target"))
        hook.jobFinish should be (Some("job_finish/$job/$target"))
        hook.jobSuccess should be (Some("job_success/$job/$target"))
        hook.jobSkip should be (Some("job_skip/$job/$target"))
        hook.jobFailure should be (Some("job_failure/$job/$target"))
        hook.targetStart should be (Some("target_start/$job/$target"))
        hook.targetFinish should be (Some("target_finish/$job/$target"))
        hook.targetSuccess should be (Some("target_success/$job/$target"))
        hook.targetSkip should be (Some("target_skip/$job/$target"))
        hook.targetFailure should be (Some("target_failure/$job/$target"))

        session.shutdown()
    }

    it should "be deserializable in a job" in {
        val spec =
            """
              |jobs:
              |  main:
              |    hooks:
              |      - kind: web
              |        jobStart: job_start/$job/$target
              |        jobFinish: job_finish/$job/$target
              |        jobSuccess: job_success/$job/$target
              |        jobSkip: job_skip/$job/$target
              |        jobFailure: job_failure/$job/$target
              |        targetStart: target_start/$job/$target
              |        targetFinish: target_finish/$job/$target
              |        targetSuccess: target_success/$job/$target
              |        targetSkip: target_skip/$job/$target
              |        targetFailure: target_failure/$job/$target
              |""".stripMargin
        val session = Session.builder()
            .disableSpark()
            .build()
        val job = Module.read.string(spec)
            .toProject("project")
            .jobs("main")
            .instantiate(session.context)

        val hook = job.hooks.head.instantiate(session.context).asInstanceOf[WebHook]
        hook.jobStart should be(Some("job_start/$job/$target"))
        hook.jobFinish should be(Some("job_finish/$job/$target"))
        hook.jobSuccess should be(Some("job_success/$job/$target"))
        hook.jobSkip should be(Some("job_skip/$job/$target"))
        hook.jobFailure should be(Some("job_failure/$job/$target"))
        hook.targetStart should be(Some("target_start/$job/$target"))
        hook.targetFinish should be(Some("target_finish/$job/$target"))
        hook.targetSuccess should be(Some("target_success/$job/$target"))
        hook.targetSkip should be(Some("target_skip/$job/$target"))
        hook.targetFailure should be(Some("target_failure/$job/$target"))

        session.shutdown()
    }

    it should "work inside a namespace and job" in {
        val namespaceHook = new Prototype[Hook] {
            override def instantiate(context: Context, properties:Option[Hook.Properties]): Hook = WebHook(
                Hook.Properties(context),
                jobStart = Some("http://0.0.0.0/$env/$job"),
                jobFinish = Some("http://0.0.0.0/$env/$job"),
                targetStart = Some("http://0.0.0.0/$env/$job/$target"),
                targetFinish = Some("http://0.0.0.0/$env/$job/$target")
            )
        }
        val jobHook = new Prototype[Hook] {
            override def instantiate(context: Context, properties:Option[Hook.Properties]): Hook = WebHook(
                Hook.Properties(context),
                jobStart = Some("http://0.0.0.0/$env/$name/$arg1"),
                jobFinish = Some("http://0.0.0.0/$env/$name/$arg1"),
                targetStart = Some("http://0.0.0.0/$env/$job/$target/$arg1"),
                targetFinish = Some("http://0.0.0.0/$env/$job/$target/$arg1")
            )
        }
        val ns = Namespace(
            name = "default",
            hooks = Seq(namespaceHook)
        )
        val project = Project(
            name = "default",
            targets = Map("t0" -> EmptyTargetSpec("t0"))
        )
        val session = Session.builder()
            .withSparkSession(spark)
            .withNamespace(ns)
            .withEnvironment("env", "some_env")
            .withProject(project)
            .build()

        val job = Job.builder(session.getContext(project))
            .setName("job")
            .addHook(jobHook)
            .addTarget(TargetIdentifier("t0"))
            .addParameter("arg1", StringType)
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.CREATE), Map("arg1" -> "some_arg"), force=true) should be (Status.SUCCESS)

        session.shutdown()
    }
}
