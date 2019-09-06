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
import java.sql.Timestamp
import java.time.Clock

import scala.annotation.tailrec

import javax.xml.bind.DatatypeConverter
import org.slf4j.LoggerFactory
import slick.jdbc.DerbyProfile
import slick.jdbc.H2Profile
import slick.jdbc.MySQLProfile
import slick.jdbc.PostgresProfile

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status



object JdbcStateStore {
    case class Connection(
        url:String,
        driver:String,
        user:String = "",
        password:String = "",
        properties: Map[String,String] = Map()
    )
}


class JdbcStateStore(connection:JdbcStateStore.Connection, retries:Int=3, timeout:Int=1000) extends StateStore {
    import JdbcStateRepository._

    private val logger = LoggerFactory.getLogger(classOf[JdbcStateStore])

    /**
      * Returns the state of a job, or None if no information is available
      * @param batch
      * @return
      */
    override def getBatchState(batch: BatchInstance, phase: Phase): Option[BatchState] = {
        val run =  BatchRun(
            0,
            Option(batch.namespace).getOrElse(""),
            Option(batch.project).getOrElse(""),
            batch.getBatch,
            phase.value,
            hashArgs(batch),
            null,
            null,
            null
        )
        logger.info(s"Checking last state for phase '${phase}' of bundle ${run.namespace}/${run.project}/${run.bundle} in state database")
        withSession { repository =>
            repository.getBatchState(run)
        }
    }

    /**
      * Starts the run and returns a token, which can be anything
      * @param batch
      * @return
      */
    override def startBatch(batch:BatchInstance, phase: Phase) : BatchToken = {
        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        val run =  BatchRun(
            0,
            Option(batch.namespace).getOrElse(""),
            Option(batch.project).getOrElse(""),
            batch.getBatch,
            phase.value,
            hashArgs(batch),
            now,
            new Timestamp(0),
            Status.RUNNING.value
        )

        logger.info(s"Writing startJob marker for phase '${phase}' of bundle ${run.namespace}/${run.project}/${run.bundle} into state database")
        withSession { repository =>
            repository.insertBatchRun(run, batch.args)
        }
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    override def finishBatch(token:BatchToken, status: Status) : Unit = {
        val run = token.asInstanceOf[BatchRun]
        logger.info(s"Mark last run of job ${run.namespace}/${run.project}/${run.bundle} as $status in state database")

        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        withSession{ repository =>
            // Library.setState(run.copy(end_ts = now, status=status))
            repository.setBatchStatus(run.copy(end_ts = now, status=status.value))
        }
    }

    /**
      * Returns the state of a specific target on its last run, or None if no information is available
      * @param target
      * @return
      */
    override def getTargetState(target:TargetInstance, phase: Phase) : Option[TargetState] = {
        val run =  TargetRun(
            0,
            None,
            Option(target.namespace).getOrElse(""),
            Option(target.project).getOrElse(""),
            target.target,
            phase.value,
            hashPartitions(target),
            null,
            null,
            null
        )
        logger.info(s"Checking last state for phase '$phase' of target ${run.namespace}/${run.project}/${run.target} in state database")
        withSession { repository =>
            repository.getTargetState(run, target.partitions)
        }
    }

    /**
      * Performs some checks, if the run is required
      * @param target
      * @return
      */
    override def checkTarget(target:TargetInstance, phase: Phase) : Boolean = {
        val state = getTargetState(target, phase).map(_.status)
        state match {
            case Some(Status.SUCCESS) => true
            case Some(_) => false
            case None => false
        }
    }

    /**
      * Starts the run and returns a token, which can be anything
      * @param target
      * @return
      */
    override def startTarget(target:TargetInstance, phase: Phase, parent:Option[BatchToken]) : TargetToken = {
        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        val run =  TargetRun(
            0,
            parent.map(_.asInstanceOf[BatchRun].id),
            Option(target.namespace).getOrElse(""),
            Option(target.project).getOrElse(""),
            target.target,
            phase.value,
            hashPartitions(target),
            now,
            new Timestamp(0),
            Status.RUNNING.value
        )

        logger.info(s"Writing start marker for phase '$phase' of target ${run.namespace}/${run.project}/${run.target} into state database")
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
        logger.info(s"Mark last run of target ${run.namespace}/${run.project}/${run.target} as $status in state database")

        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        withSession{ repository =>
            // Library.setState(run.copy(end_ts = now, status=status))
            repository.setTargetStatus(run.copy(end_ts = now, status=status.value))
        }
    }

    /**
      * Returns a list of job matching the query criteria
      * @param query
      * @param limit
      * @param offset
      * @return
      */
    override def findBundles(query:BatchQuery, order:Seq[BatchOrder], limit:Int, offset:Int) : Seq[BatchState] = Seq()

    /**
      * Returns a list of job matching the query criteria
      * @param query
      * @param limit
      * @param offset
      * @return
      */
    override def findTargets(query:TargetQuery, order:Seq[TargetOrder], limit:Int, offset:Int) : Seq[TargetState] = Seq()

    private def hashArgs(job:BatchInstance) : String = {
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
        @tailrec
        def retry[T](n:Int)(fn: => T) : T = {
            try {
                fn
            } catch {
                case e: SQLRecoverableException if n > 1 => {
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
        val mysqlPattern = """.*\.mysql\..*""".r
        val postgresqlPattern = """.*\.postgresql\..*""".r
        val profile = connection.driver match {
            case derbyPattern() => DerbyProfile
            case h2Pattern() => H2Profile
            case mysqlPattern() => MySQLProfile
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
