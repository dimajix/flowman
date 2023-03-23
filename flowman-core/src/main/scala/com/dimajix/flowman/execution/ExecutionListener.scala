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

package com.dimajix.flowman.execution

import com.dimajix.flowman.documentation.Documenter
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.DocumenterResult
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobLifecycle
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.LifecycleResult
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.Measure
import com.dimajix.flowman.model.MeasureResult
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult


abstract class Token
abstract class LifecycleToken extends Token
abstract class JobToken extends Token
abstract class TargetToken extends Token
abstract class TestToken extends Token
abstract class AssertionToken extends Token
abstract class MeasureToken extends Token
abstract class DocumenterToken extends Token


trait ExecutionListener {
    /**
     * Starts the run and returns a token, which can be anything
     * @param job
     * @return
     */
    def startLifecycle(execution:Execution, job:Job, instance:JobLifecycle) : LifecycleToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    def finishLifecycle(execution:Execution, token:LifecycleToken, result:LifecycleResult) : Unit

    /**
     * Starts the run and returns a token, which can be anything
     * @param job
     * @return
     */
    def startJob(execution:Execution, job:Job, instance:JobDigest, parent:Option[Token]) : JobToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    def finishJob(execution:Execution, token:JobToken, result:JobResult) : Unit

    /**
     * Starts the run and returns a token, which can be anything
     * @param target
     * @return
     */
    def startTarget(execution:Execution, target:Target, instance:TargetDigest, parent:Option[Token]) : TargetToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    def finishTarget(execution:Execution, token:TargetToken, result:TargetResult) : Unit

    /**
     * Starts the assertion and returns a token, which can be anything
     * @param assertion
     * @return
     */
    def startAssertion(execution:Execution, assertion:Assertion, parent:Option[Token]) : AssertionToken

    /**
     * Sets the status of a assertion after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    def finishAssertion(execution:Execution, token:AssertionToken, result:AssertionResult) : Unit

    /**
     * Starts the measure and returns a token, which can be anything
     * @param measure
     * @return
     */
    def startMeasure(execution:Execution, measure:Measure, parent:Option[Token]) : MeasureToken

    /**
     * Sets the status of a measure after it has been started
     * @param token The token returned by startJob
     * @param result
     */
    def finishMeasure(execution:Execution, token:MeasureToken, result:MeasureResult) : Unit

    /**
     * Starts the documenter and returns a token, which can be anything
     *
     * @param documenter
     * @return
     */
    def startDocumenter(execution: Execution, documenter: Documenter, parent: Option[Token]): DocumenterToken

    /**
     * Sets the status of a documenter after it has been started
     *
     * @param token The token returned by startJob
     * @param result
     */
    def finishDocumenter(execution: Execution, token: DocumenterToken, result: DocumenterResult): Unit

    /**
     * Informs the listener that a specific mapping is about to be instantiated
     * @param execution
     * @param mapping
     * @param parent
     */
    def instantiateMapping(execution: Execution, mapping:Mapping, parent:Option[Token]) : Unit

    /**
     * Informs the listener that a specific mapping is about to be described
     * @param execution
     * @param mapping
     * @param parent
     */
    def describeMapping(execution: Execution, mapping:Mapping, parent:Option[Token]) : Unit

    /**
     * Informs the listener that a specific relation is about to be described
     * @param execution
     * @param relation
     * @param parent
     */
    def describeRelation(execution: Execution, relation:Relation, parent:Option[Token]) : Unit
}


abstract class AbstractExecutionListener extends ExecutionListener {
    override def startLifecycle(execution:Execution, job:Job, instance:JobLifecycle) : LifecycleToken = new LifecycleToken {}
    override def finishLifecycle(execution:Execution, token:LifecycleToken, result:LifecycleResult) : Unit = {}
    override def startJob(execution:Execution, job: Job, instance: JobDigest, parent:Option[Token]): JobToken = new JobToken {}
    override def finishJob(execution:Execution, token: JobToken, result: JobResult): Unit = {}
    override def startTarget(execution:Execution, target: Target, instance:TargetDigest, parent: Option[Token]): TargetToken = new TargetToken {}
    override def finishTarget(execution:Execution, token: TargetToken, result: TargetResult): Unit = {}
    override def startAssertion(execution:Execution, assertion: Assertion, parent: Option[Token]): AssertionToken = new AssertionToken {}
    override def finishAssertion(execution:Execution, token: AssertionToken, result: AssertionResult): Unit = {}
    override def startMeasure(execution:Execution, measure: Measure, parent: Option[Token]): MeasureToken = new MeasureToken {}
    override def finishMeasure(execution:Execution, token: MeasureToken, result: MeasureResult): Unit = {}
    override def startDocumenter(execution: Execution, documenter: Documenter, parent: Option[Token]): DocumenterToken = new DocumenterToken {}
    override def finishDocumenter(execution: Execution, token: DocumenterToken, result: DocumenterResult): Unit = {}
    override def instantiateMapping(execution: Execution, mapping:Mapping, parent:Option[Token]) : Unit = {}
    override def describeMapping(execution: Execution, mapping:Mapping, parent:Option[Token]) : Unit = {}
    override def describeRelation(execution: Execution, relation:Relation, parent:Option[Token]) : Unit = {}
}
