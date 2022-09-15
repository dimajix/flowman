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

package com.dimajix.flowman.history

import java.security.MessageDigest
import java.sql.SQLRecoverableException
import java.sql.SQLTransientException
import java.sql.Timestamp
import java.time.Clock

import javax.xml.bind.DatatypeConverter
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.graph.GraphBuilder
import com.dimajix.flowman.history.JdbcStateRepository.JobRun
import com.dimajix.flowman.history.JdbcStateRepository.TargetRun
import com.dimajix.flowman.history.JdbcStateStore.JdbcJobToken
import com.dimajix.flowman.history.JdbcStateStore.JdbcTargetToken
import com.dimajix.flowman.jdbc.SlickUtils
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.spi.LogFilter



object JdbcStateStore {
    case class Connection(
        url:String,
        driver:String,
        user:Option[String] = None,
        password:Option[String] = None,
        properties: Map[String,String] = Map()
    )

    case class JdbcTargetToken(
        run:TargetRun,
        parent:Option[JdbcJobToken]
    ) extends TargetToken

    case class JdbcJobToken(
        run:JobRun,
        graph:GraphBuilder
    ) extends JobToken
}


case class JdbcStateStore(connection:JdbcStateStore.Connection, retries:Int=3, timeout:Int=1000) extends AbstractStateStore {
    import JdbcStateRepository._

    private val logger = LoggerFactory.getLogger(classOf[JdbcStateStore])
    private val logFilters = LogFilter.filters

    /**
      * Returns the state of a job, or None if no information is available
      * @param job
      * @return
      */
    override def getJobState(job: JobDigest): Option[JobState] = {
        val run = JobRun(
            0,
            job.namespace,
            job.project,
            null,
            job.job,
            null,
            hashArgs(job),
            None,
            None,
            null,
            None
        )
        logger.debug(s"Checking last state of '${run.phase}' job '${run.name}' in history database")
        withRepository { repository =>
            repository.getJobState(run)
        }
    }

    /**
     * Returns all metrics belonging to a specific job instance
     * @param jobId
     * @return
     */
    override def getJobMetrics(jobId:String) : Seq[Measurement] = {
        withRepository { repository =>
            repository.getJobMetrics(jobId.toLong)
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
            repository.getJobGraph(jobId.toLong)
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
            repository.getJobEnvironment(jobId.toLong)
        }
    }

    /**
      * Starts the run and returns a token, which can be anything
     *
     * @param digest
      * @return
      */
    override def startJob(job:Job, digest:JobDigest) : JobToken = {
        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        val run =  JobRun(
            0,
            job.namespace.map(_.name).getOrElse(""),
            job.project.map(_.name).getOrElse(""),
            job.project.flatMap(_.version).getOrElse(""),
            job.name,
            digest.phase.upper,
            hashArgs(digest),
            Some(now),
            None,
            Status.RUNNING.upper,
            None
        )

        // Create redacted environment
        val env = job.context.environment.toMap
            .flatMap { case(key,value) =>
                LogFilter.filter(logFilters, key, value.toString)
            }

        logger.debug(s"Start '${digest.phase}' job '${run.name}' in history database")
        val run2 = withRepository { repository =>
            repository.insertJobRun(run, digest.args, env)
        }

        JdbcJobToken(run2, new GraphBuilder(job.context, digest.phase))
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    override def finishJob(token:JobToken, result: JobResult, metrics:Seq[Measurement]=Seq()) : Unit = {
        val status = result.status
        val jdbcToken = token.asInstanceOf[JdbcJobToken]
        val run = jdbcToken.run
        logger.info(s"Mark '${run.phase}' job '${run.name}' as $status in history database")

        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        val graph = Graph.ofGraph(jdbcToken.graph.build())
        withRepository{ repository =>
            repository.setJobStatus(run.copy(end_ts = Some(now), status=status.upper, error=result.exception.map(reasons)))
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
        val run = TargetRun(
            0,
            None,
            target.namespace,
            target.project,
            null,
            target.target,
            null,
            hashPartitions(target),
            None,
            None,
            null,
            None
        )
        logger.debug(s"Checking state of target '${run.name}' in history database")
        withRepository { repository =>
            repository.getTargetState(run, target.partitions)
        }
    }

    def getTargetState(targetId: String): TargetState = {
        withRepository { repository =>
            repository.getTargetState(targetId.toLong)
        }
    }

    /**
      * Starts the run and returns a token, which can be anything
      * @param digest
      * @return
      */
    override def startTarget(target:Target, digest:TargetDigest, parent:Option[JobToken]) : TargetToken = {
        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)

        val parentRun = parent.map(_.asInstanceOf[JdbcJobToken])
        val run =  TargetRun(
            0,
            parentRun.map(_.run.id),
            target.namespace.map(_.name).getOrElse(""),
            target.project.map(_.name).getOrElse(""),
            target.project.flatMap(_.version).getOrElse(""),
            target.name,
            digest.phase.upper,
            hashPartitions(digest),
            Some(now),
            None,
            Status.RUNNING.upper,
            None
        )

        logger.debug(s"Start '${digest.phase}' target '${run.name}' in history database")
        val run2 = withRepository { repository =>
            repository.insertTargetRun(run, digest.partitions)
        }
        JdbcTargetToken(run2, parentRun)
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    override def finishTarget(token:TargetToken, result: TargetResult) : Unit = {
        val status = result.status
        val jdbcToken = token.asInstanceOf[JdbcTargetToken]
        val run = jdbcToken.run
        logger.info(s"Mark '${run.phase}' target '${run.name}' as $status in history database")

        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        withRepository{ repository =>
            repository.setTargetStatus(run.copy(end_ts = Some(now), status=status.upper, error=result.exception.map(reasons)))
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
    override def findTargets(query:TargetQuery, order:Seq[TargetOrder]=Seq(), limit:Int=10000, offset:Int=0) : Seq[TargetState] = {
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

    private def hashArgs(job:JobDigest) : String = {
         hashMap(job.args)
    }

    private def hashPartitions(target:TargetDigest) : String = {
        hashMap(target.partitions)
    }

    private def hashMap(map:Map[String,String]) : String = {
        val strArgs = map.map(kv => kv._1 + "=" + kv._2).mkString(",")
        val bytes = strArgs.getBytes("UTF-8")
        val digest = MessageDigest.getInstance("MD5").digest(bytes)
        DatatypeConverter.printHexBinary(digest).toUpperCase()
    }

    /**
      * Performs some a task with a JDBC session, also automatically performing retries and timeouts
      *
      * @param query
      * @tparam T
      * @return
      */
    private def withRepository[T](query: JdbcStateRepository => T) : T = {
        def retry[T](n:Int)(fn: => T) : T = {
            try {
                fn
            } catch {
                case e @(_:SQLRecoverableException|_:SQLTransientException) if n > 1 => {
                    logger.warn("Retrying after error while executing SQL: {}", e.getMessage)
                    Thread.sleep(timeout)
                    retry(n - 1)(fn)
                }
            }
        }

        retry(retries) {
            ensureTables()
            query(repository)
        }
    }

    private var tablesCreated:Boolean = false
    private lazy val repository = new JdbcStateRepository(connection, SlickUtils.getProfile(connection.driver))

    private def ensureTables() : Unit = {
        // Create Database if not exists
        if (!tablesCreated) {
            repository.create()
            tablesCreated = true
        }
    }

}
