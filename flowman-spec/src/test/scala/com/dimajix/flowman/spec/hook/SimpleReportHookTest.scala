/*
 * Copyright 2021 Kaya Kupferschmidt
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

import org.apache.hadoop.fs.Path
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.LifecycleResult
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.target.NullTarget
import com.dimajix.spark.testing.LocalTempDir


class SimpleReportHookTest extends AnyFlatSpec with Matchers with MockFactory with LocalTempDir {
    "The SimpleReportHook" should "be parseable" in {
        val spec =
            """
              |kind: simpleReport
              |location: file:///tmp/some-report.txt
              |mode: overwrite
              |""".stripMargin

        val session = Session.builder()
            .disableSpark()
            .build()
        val hookSpec = ObjectMapper.parse[HookSpec](spec)
        val hook = hookSpec.instantiate(session.context).asInstanceOf[SimpleReportHook]
        hook.location should be (new Path("file:///tmp/some-report.txt"))
        hook.mode should be (OutputMode.OVERWRITE)
    }

    it should "work" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        val context = session.context

        val hook = SimpleReportHook(
            Hook.Properties(context),
            new Path(tempDir.getPath, "some-other-report.txt")
        )

        val job = Job.builder(context).setName("some_job").build()
        val target = NullTarget(Target.Properties(context, "null_target"), Map())
        val assertion = mock[Assertion]
        (assertion.name _).expects().anyNumberOfTimes().returns("some_assertion")
        val jobInstance = job.instance(Map())
        val lifecycle = Seq(Phase.VALIDATE, Phase.CREATE, Phase.BUILD, Phase.VERIFY)
        val lifecycleToken = hook.startLifecycle(job, jobInstance, lifecycle)

        //==== VALIDATE ===============================================================================================
        val jobTokenVALIDATE = hook.startJob(job, jobInstance, Phase.VALIDATE, Some(lifecycleToken))
        val targetTokenVALIDATE = hook.startTarget(target, target.instance, Phase.VALIDATE, Some(jobTokenVALIDATE))
        hook.finishTarget(targetTokenVALIDATE, TargetResult(target, Phase.VALIDATE, Status.SUCCESS, Instant.now()))
        hook.finishJob(jobTokenVALIDATE, JobResult(job, jobInstance, Phase.VALIDATE, Status.SUCCESS, Instant.now()))

        //==== CREATE =================================================================================================
        val jobTokenCREATE = hook.startJob(job, jobInstance, Phase.CREATE, Some(lifecycleToken))
        val targetTokenCREATE = hook.startTarget(target, target.instance, Phase.CREATE, Some(jobTokenCREATE))
        hook.finishTarget(targetTokenCREATE, TargetResult(target, Phase.CREATE, Status.SUCCESS, Instant.now()))
        hook.finishJob(jobTokenCREATE, JobResult(job, jobInstance, Phase.CREATE, Status.SUCCESS, Instant.now()))

        //==== BUILD =================================================================================================
        val jobTokenBUILD = hook.startJob(job, jobInstance, Phase.BUILD, Some(lifecycleToken))
        val targetTokenBUILD = hook.startTarget(target, target.instance, Phase.BUILD, Some(jobTokenBUILD))
        hook.finishTarget(targetTokenBUILD, TargetResult(target, Phase.BUILD, Status.SUCCESS, Instant.now()))
        hook.finishJob(jobTokenBUILD, JobResult(job, jobInstance, Phase.BUILD, Status.SUCCESS, Instant.now()))

        //==== VERIFY =================================================================================================
        val jobTokenVERIFY = hook.startJob(job, jobInstance, Phase.VERIFY, Some(lifecycleToken))
        val targetTokenVERIFY = hook.startTarget(target, target.instance, Phase.VERIFY, Some(jobTokenVERIFY))
        val assertionToken = hook.startAssertion(assertion, Some(targetTokenVERIFY))
        hook.finishAssertion(assertionToken, AssertionResult(assertion, Seq(), Instant.now()))
        hook.finishTarget(targetTokenVERIFY, TargetResult(target, Phase.VERIFY, Status.SUCCESS, Instant.now()))
        hook.finishJob(jobTokenVERIFY, JobResult(job, jobInstance, Phase.VERIFY, Status.SUCCESS, Instant.now()))

        hook.finishLifecycle(lifecycleToken, LifecycleResult(job, jobInstance, lifecycle, Status.SUCCESS, Instant.now()))
    }
}
