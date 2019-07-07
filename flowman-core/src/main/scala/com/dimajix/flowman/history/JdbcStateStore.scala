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
import java.time.ZoneId
import java.util.Locale
import java.util.Properties

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import javax.xml.bind.DatatypeConverter
import org.slf4j.LoggerFactory
import slick.jdbc.DerbyProfile
import slick.jdbc.H2Profile
import slick.jdbc.JdbcProfile
import slick.jdbc.MySQLProfile
import slick.jdbc.PostgresProfile
import slick.jdbc.meta.MTable

import com.dimajix.flowman.history.JdbcJobRepository.TargetRun


private object JdbcJobRepository {
    private val logger = LoggerFactory.getLogger(classOf[JdbcJobRepository])

    case class JobRun(
         id:Long,
         parent_id:Option[Long],
         namespace: String,
         project:String,
         job:String,
         args_hash:String,
         start_ts:Timestamp,
         end_ts:Timestamp,
         status:String
     ) extends JobToken

    case class JobArgument(
        job_id:Long,
        name:String,
        value:String
    )

    case class TargetRun(
        id:Long,
        job_id:Option[Long],
        namespace: String,
        project:String,
        target:String,
        partitions_hash:String,
        start_ts:Timestamp,
        end_ts:Timestamp,
        status:String
    ) extends TargetToken

    case class TargetPartition(
        target_id:Long,
        name:String,
        value:String
    )
}


private class JdbcJobRepository(connection: JdbcStateStore.Connection, val profile:JdbcProfile) {
    import profile.api._

    import JdbcJobRepository._

    private lazy val db = {
        val url = connection.url
        val user = connection.user
        val password = connection.password
        val driver = connection.driver
        val props = new Properties()
        connection.properties.foreach(kv => props.setProperty(kv._1, kv._2))
        logger.info(s"Connecting via JDBC to $url with driver $driver")
        Database.forURL(url, user=user, password=password, prop=props, driver=driver)
    }

    val jobRuns = TableQuery[JobRuns]
    val jobArgs = TableQuery[JobArguments]
    val targetRuns = TableQuery[TargetRuns]
    val targetPartitions = TableQuery[TargetPartitions]

    class JobRuns(tag:Tag) extends Table[JobRun](tag, "JOB_RUN") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def parent_id = column[Option[Long]]("parent_id")
        def namespace = column[String]("namespace")
        def project = column[String]("project")
        def job = column[String]("job")
        def args_hash = column[String]("args_hash")
        def start_ts = column[Timestamp]("start_ts")
        def end_ts = column[Timestamp]("end_ts")
        def status = column[String]("status")

        def idx = index("JOB_RUN_IDX", (namespace, project, job, args_hash, status), unique = false)
        def parent_job = foreignKey("JOB_RUN_PARENT_FK", parent_id, jobRuns)(_.id.?, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (id, parent_id, namespace, project, job, args_hash, start_ts, end_ts, status) <> (JobRun.tupled, JobRun.unapply)
    }

    class JobArguments(tag: Tag) extends Table[JobArgument](tag, "JOB_ARGUMENT") {
        def job_id = column[Long]("job_id")
        def name = column[String]("name")
        def value = column[String]("value")

        def pk = primaryKey("JOB_ARGUMENT_PK", (job_id, name))
        def job = foreignKey("JOB_ARGUMENT_JOB_FK", job_id, jobRuns)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (job_id, name, value) <> (JobArgument.tupled, JobArgument.unapply)
    }

    class TargetRuns(tag: Tag) extends Table[TargetRun](tag, "TARGET_RUN") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def job_id = column[Option[Long]]("job_id")
        def namespace = column[String]("namespace")
        def project = column[String]("project")
        def target = column[String]("target")
        def partitions_hash = column[String]("partitions_hash")
        def start_ts = column[Timestamp]("start_ts")
        def end_ts = column[Timestamp]("end_ts")
        def status = column[String]("status")

        def idx = index("TARGET_RUN_IDX", (namespace, project, target, partitions_hash, status), unique = false)
        def job = foreignKey("TARGET_RUN_JOB_RUN_FK", job_id, jobRuns)(_.id.?, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (id, job_id, namespace, project, target, partitions_hash, start_ts, end_ts, status) <> (TargetRun.tupled, TargetRun.unapply)
    }

    class TargetPartitions(tag: Tag) extends Table[TargetPartition](tag, "TARGET_PARTITION") {
        def target_id = column[Long]("target_id")
        def name = column[String]("name")
        def value = column[String]("value")

        def pk = primaryKey("TARGET_PARTITION_PK", (target_id, name))
        def target = foreignKey("TARGET_PARTITION_TARGET_RUN_FK", target_id, targetRuns)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (target_id, name, value) <> (TargetPartition.tupled, TargetPartition.unapply)
    }

    def create() : Unit = {
        import scala.concurrent.ExecutionContext.Implicits.global
        val tables = Seq(jobRuns, jobArgs, targetRuns, targetPartitions)

        val existing = db.run(profile.defaultTables)
        val query = existing.flatMap( v => {
            val names = v.map(mt => mt.name.name.toLowerCase(Locale.ROOT))
            val createIfNotExist = tables
                .filter(table => !names.contains(table.baseTableRow.tableName.toLowerCase(Locale.ROOT)))
                .map(_.schema.create)
            db.run(DBIO.sequence(createIfNotExist))
        })
        Await.result(query, Duration.Inf)
    }

    def getJobState(run:JobRun) : Option[JobState] = {
        val q = jobRuns.filter(_.id === jobRuns.filter(r =>
            r.namespace === run.namespace
                && r.project === run.project
                && r.job === run.job
                && r.args_hash === run.args_hash
                && r.status =!= Status.SKIPPED.value
        ).map(_.id).max
        )
        Await.result(db.run(q.result), Duration.Inf)
            .headOption
            .map(state => JobState(
                Status.ofString(state.status),
                Option(state.start_ts).map(_.toInstant.atZone(ZoneId.of("UTC"))),
                Option(state.end_ts).map(_.toInstant.atZone(ZoneId.of("UTC")))
            ))
    }

    def setJobStatus(run:JobRun) : Unit = {
        val q = jobRuns.filter(_.id === run.id).map(r => (r.end_ts, r.status)).update((run.end_ts, run.status))
        Await.result(db.run(q), Duration.Inf)
    }

    def insertJobRun(run:JobRun, args:Map[String,String]) : JobRun = {
        val runQuery = (jobRuns returning jobRuns.map(_.id) into((run, id) => run.copy(id=id))) += run
        val runResult = Await.result(db.run(runQuery), Duration.Inf)

        val runArgs = args.map(kv => JobArgument(runResult.id, kv._1, kv._2))
        val argsQuery = jobArgs ++= runArgs
        Await.result(db.run(argsQuery), Duration.Inf)

        runResult
    }

    def getTargetState(target:TargetRun, partitions:Map[String,String]) : Option[TargetState] = {
        val latestId = targetRuns
            .filter(tr => tr.namespace === target.namespace
                && tr.project === target.project
                && tr.target === target.target
                && tr.partitions_hash === target.partitions_hash
                && tr.status =!= Status.SKIPPED.value
            )
            .map(_.id)
            .max

        // Finally select the run with the calculated ID
        val q = targetRuns.filter(r => r.id === latestId)
        Await.result(db.run(q.result), Duration.Inf)
            .headOption
            .map(state => TargetState(
                Status.ofString(state.status),
                Option(state.start_ts).map(_.toInstant.atZone(ZoneId.of("UTC"))),
                Option(state.end_ts).map(_.toInstant.atZone(ZoneId.of("UTC")))
            ))
    }

    def setTargetStatus(run:TargetRun) : Unit = {
        val q = targetRuns.filter(_.id === run.id).map(r => (r.end_ts, r.status)).update((run.end_ts, run.status))
        Await.result(db.run(q), Duration.Inf)
    }

    def insertTargetRun(run:TargetRun, partitions:Map[String,String]) : TargetRun = {
        val runQuery = (targetRuns returning targetRuns.map(_.id) into((run, id) => run.copy(id=id))) += run
        val runResult = Await.result(db.run(runQuery), Duration.Inf)

        val runPartitions = partitions.map(kv => TargetPartition(runResult.id, kv._1, kv._2))
        val argsQuery = targetPartitions ++= runPartitions
        Await.result(db.run(argsQuery), Duration.Inf)

        runResult
    }
}


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
    import com.dimajix.flowman.history.JdbcJobRepository.JobRun

    private val logger = LoggerFactory.getLogger(classOf[JdbcStateStore])

    /**
      * Returns the state of a job, or None if no information is available
      * @param job
      * @return
      */
    override def getJobState(job: JobInstance): Option[JobState] = {
        val run =  JobRun(
            0,
            None,
            Option(job.namespace).getOrElse(""),
            Option(job.project).getOrElse(""),
            job.job,
            hashArgs(job),
            null,
            null,
            null
        )
        logger.info(s"Checking last state for job ${run.namespace}/${run.project}/${run.job} in state database")
        withSession { repository =>
            repository.getJobState(run)
        }
    }

    /**
      * Performs some checkJob, if the run is required
      * @param job
      * @return
      */
    override def checkJob(job:JobInstance) : Boolean = {
        val state = getJobState(job).map(_.status)
        state match {
            case Some(Status.SUCCESS) => true
            case Some(_) => false
            case None => false
        }
    }

    /**
      * Starts the run and returns a token, which can be anything
      * @param job
      * @return
      */
    override def startJob(job:JobInstance, parent:Option[JobToken]) : JobToken = {
        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        val run =  JobRun(
            0,
            parent.map(_.asInstanceOf[JobRun].id),
            Option(job.namespace).getOrElse(""),
            Option(job.project).getOrElse(""),
            job.job,
            hashArgs(job),
            now,
            new Timestamp(0),
            Status.RUNNING.value
        )

        logger.info(s"Writing startJob marker for job ${run.namespace}/${run.project}/${run.job} into state database")
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
        logger.info(s"Mark last run of job ${run.namespace}/${run.project}/${run.job} as $status in state database")

        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        withSession{ repository =>
            // Library.setState(run.copy(end_ts = now, status=status))
            repository.setJobStatus(run.copy(end_ts = now, status=status.value))
        }
    }

    /**
      * Returns the state of a specific target on its last run, or None if no information is available
      * @param target
      * @return
      */
    def getTargetState(target:TargetInstance) : Option[TargetState] = {
        val run =  TargetRun(
            0,
            None,
            Option(target.namespace).getOrElse(""),
            Option(target.project).getOrElse(""),
            target.target,
            hashPartitions(target),
            null,
            null,
            null
        )
        logger.info(s"Checking last state for target ${run.namespace}/${run.project}/${run.target} in state database")
        withSession { repository =>
            repository.getTargetState(run, target.partitions)
        }
    }

    /**
      * Performs some checks, if the run is required
      * @param target
      * @return
      */
    override def checkTarget(target:TargetInstance) : Boolean = {
        val state = getTargetState(target).map(_.status)
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
    override def startTarget(target:TargetInstance, parent:Option[JobToken]) : TargetToken = {
        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        val run =  TargetRun(
            0,
            parent.map(_.asInstanceOf[JobRun].id),
            Option(target.namespace).getOrElse(""),
            Option(target.project).getOrElse(""),
            target.target,
            hashPartitions(target),
            now,
            new Timestamp(0),
            Status.RUNNING.value
        )

        logger.info(s"Writing startTarget marker for target ${run.namespace}/${run.project}/${run.target} into state database")
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
    private def withSession[T](query: JdbcJobRepository => T) : T = {
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

    private def newRepository() : JdbcJobRepository = {
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

        val repository = new JdbcJobRepository(connection, profile)

        // Create Database if not exists
        if (!tablesCreated) {
            repository.create()
            tablesCreated = true
        }

        repository
    }

}
