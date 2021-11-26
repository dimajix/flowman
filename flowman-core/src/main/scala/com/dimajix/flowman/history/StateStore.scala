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

import com.dimajix.flowman.execution
import com.dimajix.flowman.execution.AbstractExecutionListener
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Token
import com.dimajix.flowman.history
import com.dimajix.flowman.metric.Metric
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult


abstract class JobToken
abstract class TargetToken

abstract class StateStore {
    /**
     * Returns the state of a job, or None if no information is available
     *
     * @param job
     * @return
     */
    def getJobState(job: JobDigest): Option[JobState]

    /**
     * Returns all metrics belonging to a specific job instance
     *
     * @param jobId
     * @return
     */
    def getJobMetrics(jobId: String): Seq[Measurement]

    /**
     * Starts the run and returns a token, which can be anything
     *
     * @param job
     * @return
     */
    def startJob(job: Job, digest: JobDigest): JobToken

    /**
     * Sets the status of a job after it has been started
     *
     * @param token The token returned by startJob
     * @param status
     */
    def finishJob(token: JobToken, result: JobResult, metrics: Seq[Measurement] = Seq()): Unit

    /**
     * Returns the state of a specific target on its last run, or None if no information is available
     *
     * @param target
     * @return
     */
    def getTargetState(target: TargetDigest): Option[TargetState]

    /**
     * Returns an execution graph representing the logical data flow from sources into the target
     * @param targetId
     * @return
     */
    def getTargetGraph(targetId: String) : Option[TargetNode]

    /**
     * Starts the run and returns a token, which can be anything
     *
     * @param target
     * @return
     */
    def startTarget(target: Target, digest: TargetDigest, parent: Option[JobToken]): TargetToken

    /**
     * Sets the status of a job after it has been started
     *
     * @param token The token returned by startJob
     * @param status
     */
    def finishTarget(token: TargetToken, result: TargetResult): Unit

    /**
     * Returns a list of job matching the query criteria
     *
     * @param query
     * @param limit
     * @param offset
     * @return
     */
    def findJobs(query: JobQuery, order: Seq[JobOrder] = Seq(), limit: Int = 10000, offset: Int = 0): Seq[JobState]

    def countJobs(query: JobQuery): Int
    def countJobs(query: JobQuery, grouping: JobColumn): Map[String, Int]

    /**
     * Returns a list of job matching the query criteria
     *
     * @param query
     * @param limit
     * @param offset
     * @return
     */
    def findTargets(query: TargetQuery, order: Seq[TargetOrder] = Seq(), limit: Int = 10000, offset: Int = 0): Seq[TargetState]

    def countTargets(query: TargetQuery): Int
    def countTargets(query: TargetQuery, grouping: TargetColumn): Map[String, Int]

    def findJobMetrics(jobQuery: JobQuery, groupings: Seq[String]): Seq[MetricSeries]
}


object StateStoreAdaptorListener {
    final case class StateStoreJobToken(token:history.JobToken) extends execution.JobToken
    final case class StateStoreTargetToken(token:history.TargetToken) extends execution.TargetToken
}
final class StateStoreAdaptorListener(store:StateStore) extends AbstractExecutionListener {
    import StateStoreAdaptorListener._

    override def startJob(execution:Execution, job:Job, instance: JobDigest, parent:Option[Token]): com.dimajix.flowman.execution.JobToken = {
        execution.metrics.resetMetrics()
        StateStoreJobToken(store.startJob(job, instance))
    }
    override def finishJob(execution:Execution, token: com.dimajix.flowman.execution.JobToken, result: JobResult): Unit = {
        val status = result.status.toString
        val metrics = Measurement.ofMetrics(execution.metrics.metrics)
            .map(m => m.copy(labels = m.labels.updated("status", status)))
        val t = token.asInstanceOf[StateStoreJobToken].token
        store.finishJob(t, result, metrics)
    }
    override def startTarget(execution:Execution, target:Target, instance: TargetDigest, parent: Option[com.dimajix.flowman.execution.Token]): com.dimajix.flowman.execution.TargetToken = {
        val t = parent.map(_.asInstanceOf[StateStoreJobToken].token)
        StateStoreTargetToken(store.startTarget(target, instance, t))
    }
    override def finishTarget(execution:Execution, token: com.dimajix.flowman.execution.TargetToken, result:TargetResult): Unit = {
        val t = token.asInstanceOf[StateStoreTargetToken].token
        store.finishTarget(t, result)
    }
}
