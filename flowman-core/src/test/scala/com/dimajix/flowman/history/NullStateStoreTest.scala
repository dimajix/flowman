/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.history

import java.time.Instant

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobInstance
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetInstance
import com.dimajix.flowman.model.TargetResult


class NullStateStoreTest extends AnyFlatSpec with Matchers with MockFactory {
    "The NullStateStore" should "support batches" in {
        val context = RootContext.builder().build()
        val job = Job.builder(context).build()
        val instance = JobInstance(
            "default",
            "project",
            "job_01",
            Map()
        )

        val monitor = new NullStateStore
        monitor.getJobState(instance) should be (None)
        val token = monitor.startJob(job, instance, Phase.BUILD)
        monitor.getJobState(instance) should be (None)
        monitor.finishJob(token, JobResult(job, instance, Phase.BUILD, Status.SUCCESS, Instant.now()))
        monitor.getJobState(instance) should be (None)
    }

    it should "support targets" in {
        val instance = TargetInstance(
            "default",
            "project",
            "target_01",
            Map()
        )
        val target = mock[Target]
        (target.instance _).expects().anyNumberOfTimes().returns(instance)

        val monitor = new NullStateStore
        monitor.getTargetState(instance) should be (None)
        val token = monitor.startTarget(target, instance, Phase.BUILD)
        monitor.getTargetState(instance) should be (None)
        monitor.finishTarget(token, TargetResult(target, Phase.BUILD, Status.SUCCESS))
        monitor.getTargetState(instance) should be (None)
    }
}
