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

package com.dimajix.flowman.model

import com.dimajix.flowman.execution.AssertionToken
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.JobToken
import com.dimajix.flowman.execution.LifecycleToken
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.ExecutionListener
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


trait Hook extends Instance with ExecutionListener {
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
    override def startLifecycle(excution:Execution, job:Job, instance:JobInstance, lifecycle:Seq[Phase]) : LifecycleToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    override def finishLifecycle(excution:Execution, token:LifecycleToken, result:LifecycleResult) : Unit

    /**
     * Starts the run and returns a token, which can be anything
     * @param job
     * @return
     */
    override def startJob(excution:Execution, job:Job, instance:JobInstance, phase:Phase, parent:Option[Token]) : JobToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    override def finishJob(excution:Execution, token:JobToken, result:JobResult) : Unit

    /**
     * Starts the run and returns a token, which can be anything
     * @param target
     * @return
     */
    override def startTarget(excution:Execution, target:Target, instance:TargetInstance, phase:Phase, parent:Option[Token]) : TargetToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    override def finishTarget(excution:Execution, token:TargetToken, result:TargetResult) : Unit

    /**
     * Starts the assertion and returns a token, which can be anything
     * @param assertion
     * @return
     */
    override def startAssertion(excution:Execution, assertion:Assertion, parent:Option[Token]) : AssertionToken

    /**
     * Sets the status of a assertion after it has been started
     * @param token The token returned by startJob
     * @param status
     */
    override def finishAssertion(excution:Execution, token:AssertionToken, result:AssertionResult) : Unit
}


/**
 * Common base implementation for the Hook interface class. It contains a couple of common properties.
 */
abstract class BaseHook extends AbstractInstance with Hook {
    protected override def instanceProperties: Hook.Properties
    override def startLifecycle(excution:Execution, job:Job, instance:JobInstance, lifecycle:Seq[Phase]) : LifecycleToken = new LifecycleToken {}
    override def finishLifecycle(excution:Execution, token:LifecycleToken, result:LifecycleResult) : Unit = {}
    override def startJob(excution:Execution, job: Job, instance: JobInstance, phase: Phase, parent:Option[Token]): JobToken = new JobToken {}
    override def finishJob(excution:Execution, token: JobToken, result: JobResult): Unit = {}
    override def startTarget(excution:Execution, target: Target, instance:TargetInstance, phase: Phase, parent: Option[Token]): TargetToken = new TargetToken {}
    override def finishTarget(excution:Execution, token: TargetToken, result:TargetResult): Unit = {}
    override def startAssertion(excution:Execution, assertion: Assertion, parent: Option[Token]): AssertionToken = new AssertionToken {}
    override def finishAssertion(excution:Execution, token: AssertionToken, result:AssertionResult): Unit = {}
}
