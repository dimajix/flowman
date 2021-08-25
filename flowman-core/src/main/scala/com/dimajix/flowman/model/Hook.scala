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

package com.dimajix.flowman.model

import com.dimajix.flowman.execution.AssertionToken
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.JobToken
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.RunnerListener
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.execution.TargetToken
import com.dimajix.flowman.execution.TestToken
import com.dimajix.flowman.execution.Token


object Hook {
    object Properties {
        def apply(context: Context, name:String = "") : Properties = {
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                "",
                Map()
            )
        }
    }
    final case class Properties(
        context:Context,
        namespace:Option[Namespace],
        project:Option[Project],
        name:String,
        kind:String,
        labels:Map[String,String]
    )
    extends Instance.Properties[Properties] {
        override def withName(name: String): Properties = copy(name=name)
    }
}


trait Hook extends Instance with RunnerListener {
    /**
     * Returns the category of this resource
     * @return
     */
    final override def category: String = "hook"

    /**
     * Starts the run and returns a token, which can be anything
     * @param job
     * @return
     */
    override def startJob(job:Job, instance:JobInstance, phase:Phase) : JobToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param status
     */
    override def finishJob(token:JobToken, status:Status) : Unit

    /**
     * Starts the run and returns a token, which can be anything
     * @param target
     * @return
     */
    override def startTarget(target:Target, instance:TargetInstance, phase:Phase, parent:Option[Token]) : TargetToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param status
     */
    override def finishTarget(token:TargetToken, status:Status) : Unit

    /**
     * Starts the test and returns a token, which can be anything
     * @param test
     * @return
     */
    override def startTest(test:Test, instance:TestInstance) : TestToken

    /**
     * Sets the status of a test after it has been started
     * @param token The token returned by startJob
     * @param status
     */
    override def finishTest(token:TestToken, status:Status) : Unit

    /**
     * Starts the assertion and returns a token, which can be anything
     * @param assertion
     * @return
     */
    override def startAssertion(assertion:Assertion, parent:Option[Token]) : AssertionToken

    /**
     * Sets the status of a assertion after it has been started
     * @param token The token returned by startJob
     * @param status
     */
    override def finishAssertion(token:AssertionToken, result:AssertionResult) : Unit
}


/**
 * Common base implementation for the Hook interface class. It contains a couple of common properties.
 */
abstract class BaseHook extends AbstractInstance with Hook {
    protected override def instanceProperties: Hook.Properties

    override def startJob(job: Job, instance: JobInstance, phase: Phase): JobToken = new JobToken {}
    override def finishJob(token: JobToken, status: Status): Unit = {}
    override def startTarget(target: Target, instance:TargetInstance, phase: Phase, parent: Option[Token]): TargetToken = new TargetToken {}
    override def finishTarget(token: TargetToken, status: Status): Unit = {}
    override def startTest(test: Test, instance: TestInstance): TestToken = new TestToken {}
    override def finishTest(token: TestToken, status: Status): Unit = {}
    override def startAssertion(assertion: Assertion, parent: Option[Token]): AssertionToken = new AssertionToken {}
    override def finishAssertion(token: AssertionToken, result:AssertionResult): Unit = {}
}
