/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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
import com.dimajix.flowman.execution.ExecutionListener
import com.dimajix.flowman.execution.JobToken
import com.dimajix.flowman.execution.LifecycleToken
import com.dimajix.flowman.execution.MeasureToken
import com.dimajix.flowman.execution.TargetToken
import com.dimajix.flowman.execution.Token


object Hook {
    object Properties {
        def apply(context: Context, name:String = "", kind:String = "") : Properties = {
            Properties(
                context,
                Metadata(context, name, Category.TEST, kind)
            )
        }
    }
    final case class Properties(
        context:Context,
        metadata:Metadata
    )
    extends Instance.Properties[Properties] {
        override val namespace : Option[Namespace] = context.namespace
        override val project : Option[Project] = context.project
        override val kind : String = metadata.kind
        override val name : String  = metadata.name

        override def withName(name: String): Properties = copy(metadata=metadata.copy(name = name))
    }
}


trait Hook extends Instance with ExecutionListener {
    /**
     * Returns the category of this resource
     * @return
     */
    final override def category: Category = Category.HOOK

    /**
     * Starts the run and returns a token, which can be anything
     * @param job
     * @return
     */
    override def startLifecycle(execution:Execution, job:Job, instance:JobLifecycle) : LifecycleToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    override def finishLifecycle(execution:Execution, token:LifecycleToken, result:LifecycleResult) : Unit

    /**
     * Starts the run and returns a token, which can be anything
     * @param job
     * @return
     */
    override def startJob(execution:Execution, job:Job, instance:JobDigest, parent:Option[Token]) : JobToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    override def finishJob(execution:Execution, token:JobToken, result:JobResult) : Unit

    /**
     * Starts the run and returns a token, which can be anything
     * @param target
     * @return
     */
    override def startTarget(execution:Execution, target:Target, instance:TargetDigest, parent:Option[Token]) : TargetToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    override def finishTarget(execution:Execution, token:TargetToken, result:TargetResult) : Unit

    /**
     * Starts the assertion and returns a token, which can be anything
     * @param assertion
     * @return
     */
    override def startAssertion(execution:Execution, assertion:Assertion, parent:Option[Token]) : AssertionToken

    /**
     * Sets the status of a assertion after it has been started
     * @param token The token returned by startJob
     * @param status
     */
    override def finishAssertion(execution:Execution, token:AssertionToken, result:AssertionResult) : Unit

    /**
     * Starts the measure and returns a token, which can be anything
     * @param measure
     * @return
     */
    override def startMeasure(execution:Execution, measure:Measure, parent:Option[Token]) : MeasureToken

    /**
     * Sets the status of a measure after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    override def finishMeasure(execution:Execution, token:MeasureToken, result:MeasureResult) : Unit
}


/**
 * Common base implementation for the Hook interface class. It contains a couple of common properties.
 */
abstract class BaseHook extends AbstractInstance with Hook {
    protected override def instanceProperties: Hook.Properties
    override def startLifecycle(execution:Execution, job:Job, instance:JobLifecycle) : LifecycleToken = new LifecycleToken {}
    override def finishLifecycle(execution:Execution, token:LifecycleToken, result:LifecycleResult) : Unit = {}
    override def startJob(execution:Execution, job: Job, instance: JobDigest, parent:Option[Token]): JobToken = new JobToken {}
    override def finishJob(execution:Execution, token: JobToken, result: JobResult): Unit = {}
    override def startTarget(execution:Execution, target: Target, instance:TargetDigest, parent: Option[Token]): TargetToken = new TargetToken {}
    override def finishTarget(execution:Execution, token: TargetToken, result:TargetResult): Unit = {}
    override def startAssertion(execution:Execution, assertion: Assertion, parent: Option[Token]): AssertionToken = new AssertionToken {}
    override def finishAssertion(execution:Execution, token: AssertionToken, result:AssertionResult): Unit = {}
    override def startMeasure(execution:Execution, measure:Measure, parent:Option[Token]) : MeasureToken = new MeasureToken {}
    override def finishMeasure(execution:Execution, token:MeasureToken, result:MeasureResult) : Unit = {}
    override def instantiateMapping(execution: Execution, mapping:Mapping, parent:Option[Token]) : Unit = {}
    override def describeMapping(execution: Execution, mapping:Mapping, parent:Option[Token]) : Unit = {}
    override def describeRelation(execution: Execution, relation:Relation, parent:Option[Token]) : Unit = {}
}
