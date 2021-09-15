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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.LifecycleResult
import com.dimajix.flowman.spec.ObjectMapper


class ReportHookTest extends AnyFlatSpec with Matchers {
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

        val hook = ReportHook(
            Hook.Properties(context),
            new Path("file:///tmp/some-report.txt")
        )

        val job = Job.builder(context).build()
        val jobInstance = job.instance(Map())
        val lifecycleToken = hook.startLifecycle(job, jobInstance, Seq(Phase.CREATE))
        val jobToken = hook.startJob(job, jobInstance, Phase.CREATE, Some(lifecycleToken))
        hook.finishJob(jobToken, JobResult(job, jobInstance, Phase.CREATE, Status.SUCCESS, Instant.now()))
        hook.finishLifecycle(lifecycleToken, LifecycleResult(job, jobInstance, Seq(Phase.CREATE), Status.SUCCESS, Instant.now()))
    }
}
