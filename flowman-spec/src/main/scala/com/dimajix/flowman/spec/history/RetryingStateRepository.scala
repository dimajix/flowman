/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.flowman.spec.history

import com.dimajix.flowman.documentation.EntityDoc
import com.dimajix.flowman.documentation.ProjectDoc
import com.dimajix.flowman.history.DocumentationQuery
import com.dimajix.flowman.history.Graph
import com.dimajix.flowman.history.JobColumn
import com.dimajix.flowman.history.JobOrder
import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.history.JobState
import com.dimajix.flowman.history.Measurement
import com.dimajix.flowman.history.MetricSeries
import com.dimajix.flowman.history.TargetColumn
import com.dimajix.flowman.history.TargetOrder
import com.dimajix.flowman.history.TargetQuery
import com.dimajix.flowman.history.TargetState
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.TargetDigest


final class RetryingStateRepository(repository: StateRepository) extends StateRepository {
    override def create(): Unit = {
        repository.withRetry(repository.create())
    }

    override def withRetry[T](fn: => T): T = {
        repository.withRetry(fn)
    }

    override def getJobState(job: JobDigest): Option[JobState] = {
        repository.withRetry(repository.getJobState(job))
    }

    override def insertJobState(state: JobState, env: Map[String, String]): JobState = {
        repository.withRetry(repository.insertJobState(state, env))
    }

    override def updateJobState(state: JobState): Unit = {
        repository.withRetry(repository.updateJobState(state))
    }

    override def insertJobMetrics(jobId: String, metrics: Seq[Measurement]): Unit = {
        repository.withRetry(repository.insertJobMetrics(jobId, metrics))
    }

    override def insertJobDocumentation(jobId: String, doc: ProjectDoc): Unit = {
        repository.withRetry(repository.insertJobDocumentation(jobId, doc))
    }

    override def getJobDocumentation(jobId: String): Option[ProjectDoc] = {
        repository.withRetry(repository.getJobDocumentation(jobId))
    }

    override def getJobMetrics(jobId: String): Seq[Measurement] = {
        repository.withRetry(repository.getJobMetrics(jobId))
    }

    override def getJobEnvironment(jobId: String): Map[String, String] = {
        repository.withRetry(repository.getJobEnvironment(jobId))
    }

    override def insertJobGraph(jobId: String, graph: Graph): Unit = {
        repository.withRetry(repository.insertJobGraph(jobId, graph))
    }

    override def getJobGraph(jobId: String): Option[Graph] = {
        repository.withRetry(repository.getJobGraph(jobId))
    }

    override def findJobs(query: JobQuery, order: Seq[JobOrder], limit: Int, offset: Int): Seq[JobState] = {
        repository.withRetry(repository.findJobs(query, order, limit, offset))
    }

    override def countJobs(query: JobQuery): Int = {
        repository.withRetry(repository.countJobs(query))
    }

    override def countJobs(query: JobQuery, grouping: JobColumn): Seq[(String, Int)] = {
        repository.withRetry(repository.countJobs(query, grouping))
    }

    override def getTargetState(target: TargetDigest): Option[TargetState] = {
        repository.withRetry(repository.getTargetState(target))
    }

    override def getTargetState(targetId: String): TargetState = {
        repository.withRetry(repository.getTargetState(targetId))
    }

    override def insertTargetState(state: TargetState): TargetState = {
        repository.withRetry(repository.insertTargetState(state))
    }

    override def updateTargetState(state: TargetState): Unit = {
        repository.withRetry(repository.updateTargetState(state))
    }

    override def findTargets(query: TargetQuery, order: Seq[TargetOrder], limit: Int, offset: Int): Seq[TargetState] = {
        repository.withRetry(repository.findTargets(query, order, limit, offset))
    }

    override def countTargets(query: TargetQuery): Int = {
        repository.withRetry(repository.countTargets(query))
    }

    override def countTargets(query: TargetQuery, grouping: TargetColumn): Seq[(String, Int)] = {
        repository.withRetry(repository.countTargets(query, grouping))
    }

    override def findMetrics(query: JobQuery, groupings: Seq[String]): Seq[MetricSeries] = {
        repository.withRetry(repository.findMetrics(query, groupings))
    }

    override def findDocumentation(query: DocumentationQuery, limit:Int, offset:Int): Seq[EntityDoc] = {
        repository.withRetry(repository.findDocumentation(query))
    }
}
