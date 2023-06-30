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

package com.dimajix.flowman.spec.history

import java.time.Clock
import java.time.ZoneId

import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.graph.GraphBuilder
import com.dimajix.flowman.history.AbstractStateStore
import com.dimajix.flowman.history.Graph
import com.dimajix.flowman.history.JobColumn
import com.dimajix.flowman.history.JobOrder
import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.history.JobState
import com.dimajix.flowman.history.JobToken
import com.dimajix.flowman.history.Measurement
import com.dimajix.flowman.history.MetricSeries
import com.dimajix.flowman.history.TargetColumn
import com.dimajix.flowman.history.TargetOrder
import com.dimajix.flowman.history.TargetQuery
import com.dimajix.flowman.history.TargetState
import com.dimajix.flowman.history.TargetToken
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.spi.LogFilter



object RepositoryStateStore {
    case class RepositoryTargetToken(
        run:TargetState,
        parent:Option[RepositoryJobToken]
    ) extends TargetToken

    case class RepositoryJobToken(
        run:JobState,
        graph:GraphBuilder
    ) extends JobToken
}


abstract class RepositoryStateStore extends AbstractStateStore {
    import RepositoryStateStore._

    protected val logger = LoggerFactory.getLogger(classOf[JdbcStateStore])
    private val logFilters = LogFilter.filters

    /**
      * Returns the state of a job, or None if no information is available
      * @param job
      * @return
      */
    override def getJobState(job: JobDigest): Option[JobState] = {
        logger.debug(s"Checking last state of '${job.phase}' job '${job.job}' in history database")
        withRepository { repository =>
            repository.getJobState(job)
        }
    }

    /**
     * Returns all metrics belonging to a specific job instance
     * @param jobId
     * @return
     */
    override def getJobMetrics(jobId:String) : Seq[Measurement] = {
        withRepository { repository =>
            repository.getJobMetrics(jobId)
        }
    }

    /**
     * Returns the execution graph belonging to a specific job run
     *
     * @param jobId
     * @return
     */
    override def getJobGraph(jobId: String): Option[Graph] = {
        withRepository { repository =>
            repository.getJobGraph(jobId)
        }
    }

    /**
     * Returns the execution environment of a specific job run
     *
     * @param jobId
     * @return
     */
    override def getJobEnvironment(jobId: String): Map[String, String] = {
        withRepository { repository =>
            repository.getJobEnvironment(jobId)
        }
    }

    /**
      * Starts the run and returns a token, which can be anything
     *
     * @param digest
      * @return
      */
    override def startJob(job:Job, digest:JobDigest) : JobToken = {
        val now = Clock.systemDefaultZone().instant().atZone(ZoneId.systemDefault())
        val state = JobState(
            "",
            job.namespace.map(_.name).getOrElse(""),
            job.project.map(_.name).getOrElse(""),
            job.project.flatMap(_.version).getOrElse(""),
            job.name,
            digest.phase,
            digest.args,
            Status.RUNNING,
            Some(now),
            None
        )

        // Create redacted environment
        val env = job.context.environment.toMap
            .flatMap { case(key,value) =>
                LogFilter.filter(logFilters, key, value.toString)
            }

        logger.debug(s"Start '${digest.phase}' job '${state.job}' in history database")
        val run = withRepository { repository =>
            repository.insertJobState(state, env)
        }

        RepositoryJobToken(run, new GraphBuilder(job.context, digest.phase))
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    override def finishJob(token:JobToken, result: JobResult, metrics:Seq[Measurement]=Seq()) : Unit = {
        val status = result.status
        val jdbcToken = token.asInstanceOf[RepositoryJobToken]
        val run = jdbcToken.run
        logger.info(s"Mark '${run.phase}' job '${run.job}' as $status in history database")

        val now = Clock.systemDefaultZone().instant().atZone(ZoneId.systemDefault())
        val graph = Graph.ofGraph(jdbcToken.graph.build())
        withRepository { repository =>
            repository.updateJobState(run.copy(endDateTime=Some(now), status=status, error=result.exception.map(reasons)))
            repository.insertJobMetrics(run.id, metrics)
            repository.insertJobGraph(run.id, graph)
        }
    }

    /**
      * Returns the state of a specific target on its last run, or None if no information is available
      * @param target
      * @return
      */
    override def getTargetState(target:TargetDigest) : Option[TargetState] = {
        logger.debug(s"Checking state of target '${target.target}' in history database")
        withRepository { repository =>
            repository.getTargetState(target)
        }
    }

    def getTargetState(targetId: String): TargetState = {
        withRepository { repository =>
            repository.getTargetState(targetId)
        }
    }

    /**
      * Starts the run and returns a token, which can be anything
      * @param digest
      * @return
      */
    override def startTarget(target:Target, digest:TargetDigest, parent:Option[JobToken]) : TargetToken = {
        val now = Clock.systemDefaultZone().instant().atZone(ZoneId.systemDefault())

        val parentRun = parent.map(_.asInstanceOf[RepositoryJobToken])
        val run =  TargetState(
            "",
            parentRun.map(_.run.id),
            target.namespace.map(_.name).getOrElse(""),
            target.project.map(_.name).getOrElse(""),
            target.project.flatMap(_.version).getOrElse(""),
            target.name,
            digest.partitions,
            digest.phase,
            Status.RUNNING,
            Some(now),
            None,
            None
        )

        logger.debug(s"Start '${digest.phase}' target '${run.target}' in history database")
        val run2 = withRepository { repository =>
            repository.insertTargetState(run)
        }
        RepositoryTargetToken(run2, parentRun)
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    override def finishTarget(token:TargetToken, result: TargetResult) : Unit = {
        val status = result.status
        val jdbcToken = token.asInstanceOf[RepositoryTargetToken]
        val run = jdbcToken.run
        logger.info(s"Mark '${run.phase}' target '${run.target}' as $status in history database")

        val now = Clock.systemDefaultZone().instant().atZone(ZoneId.systemDefault())
        withRepository{ repository =>
            repository.updateTargetState(run.copy(endDateTime=Some(now), status=status, error=result.exception.map(reasons)))
        }

        // Add target to Job's build graph
        if (status != Status.SKIPPED) {
            jdbcToken.parent.foreach {
                _.graph.addTarget(result.target)
            }
        }
    }

    /**
      * Returns a list of job matching the query criteria
      * @param query
      * @param limit
      * @param offset
      * @return
      */
    override def findJobs(query:JobQuery, order:Seq[JobOrder]=Seq(), limit:Int=10000, offset:Int=0) : Seq[JobState] = {
        withRepository { repository =>
            repository.findJobs(query, order, limit, offset)
        }
    }


    override def countJobs(query: JobQuery): Int = {
        withRepository { repository =>
            repository.countJobs(query)
        }
    }


    override def countJobs(query: JobQuery, grouping: JobColumn): Map[String, Int] = {
        withRepository { repository =>
            repository.countJobs(query, grouping).toMap
        }
    }

    /**
      * Returns a list of job matching the query criteria
     *
     * @param query
      * @param limit
      * @param offset
      * @return
      */
    override def findTargets(query:TargetQuery, order:Seq[TargetOrder]=Seq.empty, limit:Int=10000, offset:Int=0) : Seq[TargetState] = {
        withRepository { repository =>
            repository.findTargets(query, order, limit, offset)
        }
    }

    override def countTargets(query: TargetQuery): Int = {
        withRepository { repository =>
            repository.countTargets(query)
        }
    }

    override def countTargets(query: TargetQuery, grouping: TargetColumn): Map[String, Int] = {
        withRepository { repository =>
            repository.countTargets(query, grouping).toMap
        }
    }

    override def findJobMetrics(jobQuery: JobQuery, groupings: Seq[String]): Seq[MetricSeries] = {
        withRepository { repository =>
            repository.findMetrics(jobQuery, groupings)
        }
    }

    def repository : StateRepository

    /**
      * Performs some a task with a JDBC session, also automatically performing retries and timeouts
      *
      * @param query
      * @tparam T
      * @return
      */
    protected def withRepository[T](query: StateRepository => T): T = {
        ensureTables()
        query(repository)
    }

    private var tablesCreated: Boolean = false

    private def ensureTables(): Unit = {
        // Create Database if not exists
        if (!tablesCreated) {
            repository.create()
            tablesCreated = true
        }
    }
}
