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

package com.dimajix.flowman.history

import java.time.Instant

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult


class NullStateStoreTest extends AnyFlatSpec with Matchers with MockFactory {
    "The NullStateStore" should "support batches" in {
        val context = RootContext.builder().build()
        val job = Job.builder(context).build()
        val instance = JobDigest(
            "default",
            "project",
            "job_01",
            Phase.BUILD,
            Map()
        )

        val monitor = new NullStateStore
        monitor.getJobState(instance) should be (None)
        val token = monitor.startJob(job, instance)
        monitor.getJobState(instance) should be (None)
        monitor.finishJob(token, JobResult(job, instance, Status.SUCCESS, Instant.now()))
        monitor.getJobState(instance) should be (None)
    }

    it should "support targets" in {
        val instance = TargetDigest(
            "default",
            "project",
            "target_01",
            Phase.BUILD,
            Map()
        )
        val target = mock[Target]
        (target.digest _).expects(Phase.BUILD).anyNumberOfTimes().returns(instance)

        val monitor = new NullStateStore
        monitor.getTargetState(instance) should be (None)
        val token = monitor.startTarget(target, instance)
        monitor.getTargetState(instance) should be (None)
        monitor.finishTarget(token, TargetResult(target, Phase.BUILD, Status.SUCCESS))
        monitor.getTargetState(instance) should be (None)
    }
}
