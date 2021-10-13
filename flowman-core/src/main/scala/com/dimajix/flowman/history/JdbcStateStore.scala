/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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
import slick.jdbc.DerbyProfile
import slick.jdbc.H2Profile
import slick.jdbc.MySQLProfile
import slick.jdbc.PostgresProfile

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.JobInstance
import com.dimajix.flowman.model.TargetInstance



object JdbcStateStore {
    case class Connection(
        url:String,
        driver:String,
        user:Option[String] = None,
        password:Option[String] = None,
        properties: Map[String,String] = Map()
    )
}


case class JdbcStateStore(connection:JdbcStateStore.Connection, retries:Int=3, timeout:Int=1000) extends StateStore {
    import JdbcStateRepository._

    private val logger = LoggerFactory.getLogger(classOf[JdbcStateStore])

    /**
      * Returns the state of a job, or None if no information is available
      * @param job
      * @return
      */
    override def getJobState(job: JobInstance): Option[JobState] = {
        val run = JobRun(
            0,
            Option(job.namespace).getOrElse(""),
            Option(job.project).getOrElse(""),
            job.job,
            null,
            hashArgs(job),
            null,
            null,
            null
        )
        logger.debug(s"Checking last state of phase '${run.phase}' of job '${run.namespace}/${run.project}/${run.job}' in state database")
        withSession { repository =>
            repository.getJobState(run)
        }
    }

    /**
      * Starts the run and returns a token, which can be anything
      * @param job
      * @return
      */
    override def startJob(job:JobInstance, phase: Phase) : JobToken = {
        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        val run =  JobRun(
            0,
            Option(job.namespace).getOrElse(""),
            Option(job.project).getOrElse(""),
            job.job,
            phase.upper,
            hashArgs(job),
            now,
            new Timestamp(0),
            Status.RUNNING.upper
        )

        logger.debug(s"Start '${phase}' job '${run.namespace}/${run.project}/${run.job}' in state database")
        withSession { repository =>
            repository.insertJobRun(run, job.args)
        }
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    override def finishJob(token:JobToken, status: Status) : Unit = {
        val run = token.asInstanceOf[JobRun]
        logger.info(s"Mark '${run.phase}' job '${run.namespace}/${run.project}/${run.job}' as $status in state database")

        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        withSession{ repository =>
            // Library.setState(run.copy(end_ts = now, status=status))
            repository.setJobStatus(run.copy(end_ts = now, status=status.upper))
        }
    }

    /**
      * Returns the state of a specific target on its last run, or None if no information is available
      * @param target
      * @return
      */
    override def getTargetState(target:TargetInstance) : Option[TargetState] = {
        val run =  TargetRun(
            0,
            None,
            Option(target.namespace).getOrElse(""),
            Option(target.project).getOrElse(""),
            target.target,
            null,
            hashPartitions(target),
            null,
            null,
            null
        )
        logger.debug(s"Checking state of target ${run.namespace}/${run.project}/${run.target} in state database")
        withSession { repository =>
            repository.getTargetState(run, target.partitions)
        }
    }

    /**
      * Starts the run and returns a token, which can be anything
      * @param target
      * @return
      */
    override def startTarget(target:TargetInstance, phase: Phase, parent:Option[JobToken]) : TargetToken = {
        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        val run =  TargetRun(
            0,
            parent.map(_.asInstanceOf[JobRun].id),
            Option(target.namespace).getOrElse(""),
            Option(target.project).getOrElse(""),
            target.target,
            phase.upper,
            hashPartitions(target),
            now,
            new Timestamp(0),
            Status.RUNNING.upper
        )

        logger.debug(s"Start '$phase' target '${run.namespace}/${run.project}/${run.target}' in state database")
        withSession { repository =>
            repository.insertTargetRun(run, target.partitions)
        }
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    override def finishTarget(token:TargetToken, status: Status) : Unit = {
        val run = token.asInstanceOf[TargetRun]
        logger.info(s"Mark '${run.phase}' target '${run.namespace}/${run.project}/${run.target}' as $status in state database")

        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        withSession{ repository =>
            // Library.setState(run.copy(end_ts = now, status=status))
            repository.setTargetStatus(run.copy(end_ts = now, status=status.upper))
        }
    }

    /**
      * Returns a list of job matching the query criteria
      * @param query
      * @param limit
      * @param offset
      * @return
      */
    override def findJobs(query:JobQuery, order:Seq[JobOrder], limit:Int, offset:Int) : Seq[JobState] = {
        withSession { repository =>
            repository.findJob(query, order, limit, offset)
        }
    }

    /**
      * Returns a list of job matching the query criteria
      * @param query
      * @param limit
      * @param offset
      * @return
      */
    override def findTargets(query:TargetQuery, order:Seq[TargetOrder], limit:Int, offset:Int) : Seq[TargetState] = {
        withSession { repository =>
            repository.findTarget(query, order, limit, offset)
        }
    }

    private def hashArgs(job:JobInstance) : String = {
         hashMap(job.args)
    }

    private def hashPartitions(target:TargetInstance) : String = {
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
    private def withSession[T](query: JdbcStateRepository => T) : T = {
        def retry[T](n:Int)(fn: => T) : T = {
            try {
                fn
            } catch {
                case e @(_:SQLRecoverableException|_:SQLTransientException) if n > 1 => {
                    logger.error("Retrying after error while executing SQL: {}", e.getMessage)
                    Thread.sleep(timeout)
                    retry(n - 1)(fn)
                }
            }
        }

        retry(retries) {
            val repository = newRepository()
            query(repository)
        }
    }

    private var tablesCreated:Boolean = false

    private def newRepository() : JdbcStateRepository = {
        // Get Connection
        val derbyPattern = """.*\.derby\..*""".r
        val h2Pattern = """.*\.h2\..*""".r
        val mariadbPattern = """.*\.mariadb\..*""".r
        val mysqlPattern = """.*\.mysql\..*""".r
        val postgresqlPattern = """.*\.postgresql\..*""".r
        val profile = connection.driver match {
            case derbyPattern() => DerbyProfile
            case h2Pattern() => H2Profile
            case mysqlPattern() => MySQLProfile
            case mariadbPattern() => MySQLProfile
            case postgresqlPattern() => PostgresProfile
            case _ => throw new UnsupportedOperationException(s"Database with driver ${connection.driver} is not supported")
        }

        val repository = new JdbcStateRepository(connection, profile)

        // Create Database if not exists
        if (!tablesCreated) {
            repository.create()
            tablesCreated = true
        }

        repository
    }

}
