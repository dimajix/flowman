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
package com.dimajix.flowman.model

import java.time.Instant

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status


class ResultTest extends AnyFlatSpec with Matchers with MockFactory {
    "The JobResult" should "count correctly without children" in {
        val session = Session.builder
            .disableSpark()
            .build()
        val context = session.context

        val job = new Job(
            Job.Properties(context)
        )
        val instance = job.digest(Phase.BUILD, Map())
        val exception = new IllegalArgumentException()

        val r1 = JobResult(job, instance, Status.SUCCESS, Instant.now())
        r1.status should be (Status.SUCCESS)
        r1.success should be (true)
        r1.failure should be (false)
        r1.skipped should be (false)
        r1.exception should be (None)
        r1.numFailures should be (0)
        r1.numSuccesses should be (0)
        r1.numExceptions should be (0)

        val r2 = JobResult(job, instance, Status.FAILED, Instant.now())
        r2.status should be (Status.FAILED)
        r2.success should be (false)
        r2.failure should be (true)
        r2.skipped should be (false)
        r2.exception should be (None)
        r2.numFailures should be (0)
        r2.numSuccesses should be (0)
        r2.numExceptions should be (0)

        val r3 = JobResult(job, instance, Status.SKIPPED, Instant.now())
        r3.status should be (Status.SKIPPED)
        r3.success should be (false)
        r3.failure should be (false)
        r3.skipped should be (true)
        r3.exception should be (None)
        r3.numFailures should be (0)
        r3.numSuccesses should be (0)
        r3.numExceptions should be (0)

        val r4 = JobResult(job, instance, exception, Instant.now())
        r4.status should be (Status.FAILED)
        r4.success should be (false)
        r4.failure should be (true)
        r4.skipped should be (false)
        r4.exception should be (Some(exception))
        r4.numFailures should be (0)
        r4.numSuccesses should be (0)
        r4.numExceptions should be (1)

        session.shutdown()
    }

    it should "count correctly with children" in {
        val session = Session.builder
            .disableSpark()
            .build()
        val context = session.context

        val job = new Job(
            Job.Properties(context)
        )
        val instance = job.digest(Phase.BUILD, Map())
        val exception = new IllegalArgumentException()

        val r1 = JobResult(job, instance, Seq(
            AssertionTestResult("a1", None, true, Instant.now()),
            AssertionTestResult("a2", None, true, Instant.now())
        ), Instant.now())
        r1.status should be (Status.SUCCESS)
        r1.success should be (true)
        r1.failure should be (false)
        r1.skipped should be (false)
        r1.exception should be (None)
        r1.numFailures should be (0)
        r1.numSuccesses should be (2)
        r1.numExceptions should be (0)

        val r2 = JobResult(job, instance, Seq(
            AssertionTestResult("a1", None, true, Instant.now()),
            AssertionTestResult("a2", None, false, Instant.now())
        ), Instant.now())
        r2.status should be (Status.FAILED)
        r2.success should be (false)
        r2.failure should be (true)
        r2.skipped should be (false)
        r2.exception should be (None)
        r2.numFailures should be (1)
        r2.numSuccesses should be (1)
        r2.numExceptions should be (0)

        val r3 = JobResult(job, instance, Seq(
            AssertionTestResult("a1", None, true, Instant.now()),
            AssertionTestResult("a2", None, exception, Instant.now())
        ), Instant.now())
        r3.status should be (Status.FAILED)
        r3.success should be (false)
        r3.failure should be (true)
        r3.skipped should be (false)
        r3.exception should be (None)
        r3.numFailures should be (1)
        r3.numSuccesses should be (1)
        r3.numExceptions should be (1)

        session.shutdown()
    }

    "The TargetResult" should "count correctly without children" in {
        val target = mock[Target]
        (target.digest _).expects(Phase.BUILD).anyNumberOfTimes().returns(TargetDigest("", "", "", Phase.BUILD, Map()))
        val exception = new IllegalArgumentException()

        val r1 = TargetResult(target, Phase.BUILD, Status.SUCCESS, Instant.now())
        r1.status should be (Status.SUCCESS)
        r1.success should be (true)
        r1.failure should be (false)
        r1.skipped should be (false)
        r1.exception should be (None)
        r1.numFailures should be (0)
        r1.numSuccesses should be (0)
        r1.numExceptions should be (0)

        val r2 = TargetResult(target, Phase.BUILD, Status.FAILED, Instant.now())
        r2.status should be (Status.FAILED)
        r2.success should be (false)
        r2.failure should be (true)
        r2.skipped should be (false)
        r2.exception should be (None)
        r2.numFailures should be (0)
        r2.numSuccesses should be (0)
        r2.numExceptions should be (0)

        val r3 = TargetResult(target, Phase.BUILD, Status.SKIPPED, Instant.now())
        r3.status should be (Status.SKIPPED)
        r3.success should be (false)
        r3.failure should be (false)
        r3.skipped should be (true)
        r3.exception should be (None)
        r3.numFailures should be (0)
        r3.numSuccesses should be (0)
        r3.numExceptions should be (0)

        val r4 = TargetResult(target, Phase.BUILD, exception, Instant.now())
        r4.status should be (Status.FAILED)
        r4.success should be (false)
        r4.failure should be (true)
        r4.skipped should be (false)
        r4.exception should be (Some(exception))
        r4.numFailures should be (0)
        r4.numSuccesses should be (0)
        r4.numExceptions should be (1)
    }

    it should "count correctly with children" in {
        val target = mock[Target]
        (target.digest _).expects(Phase.BUILD).anyNumberOfTimes().returns(TargetDigest("", "", "", Phase.BUILD, Map()))
        val exception = new IllegalArgumentException()

        val r1 = TargetResult(target, Phase.BUILD, Seq(
            AssertionTestResult("a1", None, true, Instant.now()),
            AssertionTestResult("a2", None, true, Instant.now())
        ), Instant.now())
        r1.status should be (Status.SUCCESS)
        r1.success should be (true)
        r1.failure should be (false)
        r1.skipped should be (false)
        r1.exception should be (None)
        r1.numFailures should be (0)
        r1.numSuccesses should be (2)
        r1.numExceptions should be (0)

        val r2 = TargetResult(target, Phase.BUILD, Seq(
            AssertionTestResult("a1", None, true, Instant.now()),
            AssertionTestResult("a2", None, false, Instant.now())
        ), Instant.now())
        r2.status should be (Status.FAILED)
        r2.success should be (false)
        r2.failure should be (true)
        r2.skipped should be (false)
        r2.exception should be (None)
        r2.numFailures should be (1)
        r2.numSuccesses should be (1)
        r2.numExceptions should be (0)

        val r3 = TargetResult(target, Phase.BUILD, Seq(
            AssertionTestResult("a1", None, true, Instant.now()),
            AssertionTestResult("a2", None, exception, Instant.now())
        ), Instant.now())
        r3.status should be (Status.FAILED)
        r3.success should be (false)
        r3.failure should be (true)
        r3.skipped should be (false)
        r3.exception should be (None)
        r3.numFailures should be (1)
        r3.numSuccesses should be (1)
        r3.numExceptions should be (1)
    }

    "The AssertionResult" should "count correctly without children" in {
        val assertion = mock[Assertion]
        (assertion.name _).expects().anyNumberOfTimes().returns("assertion")
        val exception = new IllegalArgumentException()

        val r4 = AssertionResult(assertion, exception, Instant.now())
        r4.status should be (Status.FAILED)
        r4.success should be (false)
        r4.failure should be (true)
        r4.skipped should be (false)
        r4.exception should be (Some(exception))
        r4.numFailures should be (0)
        r4.numSuccesses should be (0)
        r4.numExceptions should be (1)
    }

    it should "count correctly with children" in {
        val assertion = mock[Assertion]
        (assertion.name _).expects().anyNumberOfTimes().returns("assertion")
        val exception = new IllegalArgumentException()

        val r1 = AssertionResult(assertion, Seq(
            AssertionTestResult("a1", None, true, Instant.now()),
            AssertionTestResult("a2", None, true, Instant.now())
        ))
        r1.status should be (Status.SUCCESS)
        r1.success should be (true)
        r1.failure should be (false)
        r1.skipped should be (false)
        r1.exception should be (None)
        r1.numFailures should be (0)
        r1.numSuccesses should be (2)
        r1.numExceptions should be (0)

        val r2 = AssertionResult(assertion, Seq(
            AssertionTestResult("a1", None, true, Instant.now()),
            AssertionTestResult("a2", None, false, Instant.now())
        ))
        r2.status should be (Status.FAILED)
        r2.success should be (false)
        r2.failure should be (true)
        r2.skipped should be (false)
        r2.exception should be (None)
        r2.numFailures should be (1)
        r2.numSuccesses should be (1)
        r2.numExceptions should be (0)

        val r3 = AssertionResult(assertion, Seq(
            AssertionTestResult("a1", None, true, Instant.now()),
            AssertionTestResult("a2", None, exception, Instant.now())
        ))
        r3.status should be (Status.FAILED)
        r3.success should be (false)
        r3.failure should be (true)
        r3.skipped should be (false)
        r3.exception should be (None)
        r3.numFailures should be (1)
        r3.numSuccesses should be (1)
        r3.numExceptions should be (1)
    }
}
