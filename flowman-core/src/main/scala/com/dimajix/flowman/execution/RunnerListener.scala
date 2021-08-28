/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.execution

import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobInstance
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetInstance
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestInstance
import com.dimajix.flowman.model.TestResult


abstract class Token
abstract class JobToken extends Token
abstract class TargetToken extends Token
abstract class TestToken extends Token
abstract class AssertionToken extends Token


trait RunnerListener {
    /**
     * Starts the run and returns a token, which can be anything
     * @param job
     * @return
     */
    def startJob(job:Job, instance:JobInstance, phase:Phase) : JobToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param status
     */
    def finishJob(token:JobToken, status:Status) : Unit

    /**
     * Starts the run and returns a token, which can be anything
     * @param target
     * @return
     */
    def startTarget(target:Target, instance:TargetInstance, phase:Phase, parent:Option[Token]) : TargetToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param status
     */
    def finishTarget(token:TargetToken, status:Status) : Unit

    /**
     * Starts the test and returns a token, which can be anything
     * @param test
     * @return
     */
    def startTest(test:Test, instance:TestInstance) : TestToken

    /**
     * Sets the status of a test after it has been started
     * @param token The token returned by startJob
     * @param status
     */
    def finishTest(token:TestToken, result:TestResult) : Unit

    /**
     * Starts the assertion and returns a token, which can be anything
     * @param assertion
     * @return
     */
    def startAssertion(assertion:Assertion, parent:Option[Token]) : AssertionToken

    /**
     * Sets the status of a assertion after it has been started
     * @param token The token returned by startJob
     * @param status
     */
    def finishAssertion(token:AssertionToken, result:AssertionResult) : Unit
}


abstract class AbstractRunnerListener extends RunnerListener {
    override def startJob(job: Job, instance: JobInstance, phase: Phase): JobToken = new JobToken {}
    override def finishJob(token: JobToken, status: Status): Unit = {}
    override def startTarget(target: Target, instance:TargetInstance, phase: Phase, parent: Option[Token]): TargetToken = new TargetToken {}
    override def finishTarget(token: TargetToken, status: Status): Unit = {}
    override def startTest(test: Test, instance: TestInstance): TestToken = new TestToken {}
    override def finishTest(token: TestToken, result:TestResult): Unit = {}
    override def startAssertion(assertion: Assertion, parent: Option[Token]): AssertionToken = new AssertionToken {}
    override def finishAssertion(token: AssertionToken, result: AssertionResult): Unit = {}
}
