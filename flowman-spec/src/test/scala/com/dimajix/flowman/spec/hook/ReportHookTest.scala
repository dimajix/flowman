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
import com.dimajix.flowman.model.AssertionTestResult
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.LifecycleResult
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.target.NullTarget
import com.dimajix.flowman.spec.target.VerifyTarget
import com.dimajix.spark.testing.LocalTempDir


class ReportHookTest extends AnyFlatSpec with Matchers with MockFactory with LocalTempDir {
    "The ReportHook" should "be parseable" in {
        val spec =
            """
              |kind: report
              |location: file:///tmp/some-report.txt
              |mode: overwrite
              |""".stripMargin

        val session = Session.builder()
            .disableSpark()
            .build()
        val hookSpec = ObjectMapper.parse[HookSpec](spec)
        val hook = hookSpec.instantiate(session.context).asInstanceOf[ReportHook]
        hook.location should be (new Path("file:///tmp/some-report.txt"))
        hook.mode should be (OutputMode.OVERWRITE)
    }

    it should "work" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        val context = session.context
        val execution = session.execution

        val hook = ReportHook(
            Hook.Properties(context),
            new Path(tempDir.getPath, "some-other-report.txt")
            //new Path("/tmp/some-other-report.txt")
        )

        val job = Job.builder(context).setName("some_job").build()
        val target = NullTarget(Target.Properties(context, "null_target"), Map())
        val assertion = mock[Assertion]
        (assertion.name _).expects().anyNumberOfTimes().returns("some_assertion")
        (assertion.description _).expects().anyNumberOfTimes().returns(Some("Some important assertion"))
        val lifecycle = Seq(Phase.VALIDATE, Phase.CREATE, Phase.BUILD, Phase.VERIFY)
        val lifecycleToken = hook.startLifecycle(execution, job, job.lifecycle(lifecycle, Map()))

        //==== VALIDATE ===============================================================================================
        val jobTokenVALIDATE = hook.startJob(execution, job, job.digest(Phase.VALIDATE, Map()), Some(lifecycleToken))
        val targetTokenVALIDATE = hook.startTarget(execution, target, target.digest(Phase.VALIDATE), Some(jobTokenVALIDATE))
        hook.finishTarget(execution, targetTokenVALIDATE, TargetResult(target, Phase.VALIDATE, Status.SUCCESS, Instant.now()))
        hook.finishJob(execution, jobTokenVALIDATE, JobResult(job, job.digest(Phase.VALIDATE, Map()), Status.SUCCESS, Instant.now()))

        //==== CREATE =================================================================================================
        val jobTokenCREATE = hook.startJob(execution, job, job.digest(Phase.CREATE, Map()), Some(lifecycleToken))
        val targetTokenCREATE = hook.startTarget(execution, target, target.digest(Phase.CREATE), Some(jobTokenCREATE))
        hook.finishTarget(execution, targetTokenCREATE, TargetResult(target, Phase.CREATE, Status.SUCCESS, Instant.now()))
        hook.finishJob(execution, jobTokenCREATE, JobResult(job, job.digest(Phase.CREATE, Map()), Status.SUCCESS, Instant.now()))

        //==== BUILD =================================================================================================
        val jobTokenBUILD = hook.startJob(execution, job, job.digest(Phase.BUILD, Map()), Some(lifecycleToken))
        val targetTokenBUILD = hook.startTarget(execution, target, target.digest(Phase.BUILD), Some(jobTokenBUILD))
        hook.finishTarget(execution, targetTokenBUILD, TargetResult(target, Phase.BUILD, Status.SUCCESS, Instant.now()))
        hook.finishJob(execution, jobTokenBUILD, JobResult(job, job.digest(Phase.BUILD, Map()), Status.SUCCESS, Instant.now()))

        //==== VERIFY =================================================================================================
        val jobTokenVERIFY = hook.startJob(execution, job, job.digest(Phase.VERIFY, Map()), Some(lifecycleToken))
        val targetTokenVERIFY = hook.startTarget(execution, target, target.digest(Phase.VERIFY), Some(jobTokenVERIFY))
        val assertionToken = hook.startAssertion(execution, assertion, Some(targetTokenVERIFY))
        hook.finishAssertion(execution, assertionToken, AssertionResult(assertion, Seq(AssertionTestResult("test-1", None, true), AssertionTestResult("test-2", None, false)), Instant.now()))
        hook.finishTarget(execution, targetTokenVERIFY, TargetResult(target, Phase.VERIFY, Status.SUCCESS, Instant.now()))
        hook.finishJob(execution, jobTokenVERIFY, JobResult(job, job.digest(Phase.VERIFY, Map()), Status.SUCCESS, Instant.now()))

        hook.finishLifecycle(execution, lifecycleToken, LifecycleResult(job, job.lifecycle(lifecycle, Map()), Status.SUCCESS, Instant.now()))
    }
}
