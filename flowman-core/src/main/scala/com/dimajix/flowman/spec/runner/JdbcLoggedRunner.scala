/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.runner

import java.security.MessageDigest
import java.sql.SQLRecoverableException
import java.sql.Timestamp
import java.time.Clock
import java.util.Properties

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import com.fasterxml.jackson.annotation.JsonProperty
import javax.xml.bind.DatatypeConverter
import org.slf4j.LoggerFactory
import slick.jdbc.DerbyProfile
import slick.jdbc.H2Profile
import slick.jdbc.JdbcProfile
import slick.jdbc.MySQLProfile
import slick.jdbc.PostgresProfile

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.Connection
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.task.Job


object JdbcLoggedRunner {
    val STATUS_SUCCESS = "success"
    val STATUS_FAILED = "failed"
    val STATUS_RUNNING = "running"
    val STATUS_SKIPPED = "skipped"
    val STATUS_KILLED = "killed"
}

private object JdbcLoggedRepository {
    private val logger = LoggerFactory.getLogger(classOf[JdbcLoggedRunner])

    case class Run (
       id:Long,
       namespace: String,
       project:String,
       job:String,
       args_hash:String,
       start_ts:Timestamp,
       end_ts:Timestamp,
       status:String
   )
}

private class JdbcLoggedRepository(connection: Connection, val profile:JdbcProfile)(implicit context:Context) {
    import profile.api._
    import JdbcLoggedRepository._

    private lazy val db = {
        val url = connection.url
        val user = connection.username
        val password = connection.password
        val driver = connection.driver
        val props = new Properties()
        Option(connection.properties).foreach(_.foreach(kv => props.setProperty(kv._1, kv._2)))

        logger.info(s"Connecting via JDBC to $url")
        Database.forURL(url, user=user, password=password, prop=props, driver=driver)
    }

    class Runs(tag:Tag) extends Table[Run](tag, "JOB_RUN") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def namespace = column[String]("namespace")
        def project = column[String]("project")
        def job = column[String]("job")
        def args_hash = column[String]("args_hash")
        def start_ts = column[Timestamp]("start_ts")
        def end_ts = column[Timestamp]("end_ts")
        def status = column[String]("status")

        def idx = index("idx_jobs", (namespace, project, job, args_hash, status), unique = false)

        def * = (id, namespace, project, job, args_hash, start_ts, end_ts, status) <> (Run.tupled, Run.unapply)
    }

    val runs = TableQuery[Runs]

    def create() : Unit = {
        Await.result(db.run(runs.schema.create), Duration.Inf)
    }

    def getStatus(run:Run) : Option[String] = {
        val q = runs.filter(_.id === runs.filter( r =>
                    r.namespace === run.namespace
                    && r.project === run.project
                    && r.job === run.job
                    && r.args_hash === run.args_hash
                    && r.status =!= JdbcLoggedRunner.STATUS_SKIPPED
                ).map(_.id).max
            )
            .map(_.status)
        Await.result(db.run(q.result), Duration.Inf).headOption
    }

    def setStatus(run:Run) : Unit = {
        val q = runs.filter(_.id === run.id).map(r => (r.end_ts, r.status)).update((run.end_ts, run.status))
        Await.result(db.run(q), Duration.Inf)
    }

    def insertRun(run:Run) : Run = {
        val q = (runs returning runs.map(_.id) into((run, id) => run.copy(id=id))) += run
        Await.result(db.run(q), Duration.Inf)
    }
}


class JdbcLoggedRunner extends AbstractRunner {
    import JdbcLoggedRepository._
    import JdbcLoggedRunner._

    override protected val logger = LoggerFactory.getLogger(classOf[JdbcLoggedRunner])

    @JsonProperty(value="connection", required=true) private var _connection:String = ""
    @JsonProperty(value="retries", required=false) private var _retries:String = "3"
    @JsonProperty(value="timeout", required=false) private var _timeout:String = "1000"

    def connection(implicit context: Context) : ConnectionIdentifier = ConnectionIdentifier.parse(context.evaluate(_connection))
    def retries(implicit context: Context) : Int = context.evaluate(_retries).toInt
    def timeout(implicit context: Context) : Int = context.evaluate(_timeout).toInt

    protected override def check(context:Context, job:Job, args:Map[String,String]) : Boolean = {
        val run =  Run(
            0,
            Option(context.namespace).map(_.name).getOrElse(""),
            Option(context.project).map(_.name).getOrElse(""),
            job.name,
            hashArgs(job, args),
            null,
            null,
            null
        )
        logger.info(s"Checking last state for job ${run.namespace}/${run.project}/${run.job} in state database")
        val result =
            withSession(context) { repository =>
                repository.getStatus(run)
            }

        result match {
            case Some(status) => status == STATUS_SUCCESS
            case None => false
        }
    }

    protected override def start(context:Context, job:Job, args:Map[String,String]) : Object = {
        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        val run =  Run(
            0,
            Option(context.namespace).map(_.name).getOrElse(""),
            Option(context.project).map(_.name).getOrElse(""),
            job.name,
            hashArgs(job, args),
            now,
            new Timestamp(0),
            STATUS_RUNNING
        )

        logger.info(s"Writing start marker for job ${run.namespace}/${run.project}/${run.job} into state database")
        withSession(context) { repository =>
            repository.insertRun(run)
        }
    }

    protected override def success(context: Context, token:Object) : Unit = {
        val run = token.asInstanceOf[Run]
        setState(context, run, STATUS_SUCCESS)
    }

    protected override def failure(context: Context, token:Object) : Unit = {
        val run = token.asInstanceOf[Run]
        setState(context, run, STATUS_FAILED)
    }

    protected override def aborted(context: Context, token:Object) : Unit = {
        val run = token.asInstanceOf[Run]
        setState(context, run, STATUS_KILLED)
    }

    protected override def skipped(context: Context, token:Object) : Unit = {
        val run = token.asInstanceOf[Run]
        setState(context, run, STATUS_SKIPPED)
    }

    private def hashArgs(job:Job, args:Map[String,String]) : String = {
        val strArgs = args.map(kv => kv._1 + "=" + kv._2).mkString(",")
        val bytes = strArgs.getBytes("UTF-8")
        val digest = MessageDigest.getInstance("MD5").digest(bytes)
        DatatypeConverter.printHexBinary(digest).toUpperCase()
    }

    /**
      * Sets the final state for a specific run
      *
      * @param context
      * @param run
      * @param status
      */
    private def setState(context: Context, run: Run, status:String): Unit = {
        logger.info(s"Mark last run of job ${run.namespace}/${run.project}/${run.job} as $status in state database")
        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)
        withSession(context) { repository =>
            // Library.setState(run.copy(end_ts = now, status=status))
            repository.setStatus(run.copy(end_ts = now, status=status))
        }
    }


    /**
      * Performs some a task with a JDBC session, also automatically performing retries and timeouts
      *
      * @param context
      * @param query
      * @tparam T
      * @return
      */
    private def withSession[T](context: Context)(query: JdbcLoggedRepository => T) : T = {
        implicit val icontext = context.root


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
            val repository = newRepository(icontext)
            query(repository)
        }
    }

    private var tablesCreated:Boolean = false

    private def newRepository(context: Context) : JdbcLoggedRepository = {
        implicit val icontext = context

        // Get Connection
        val connection = context.getConnection(this.connection)
        if (connection == null)
            throw new NoSuchElementException(s"Connection '${this.connection}' not defined.")

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

        val repository = new JdbcLoggedRepository(connection, profile)

        // Create Database if not exists
        if (!tablesCreated) {
            try {
                repository.create()
            }
            catch {
                case _:Exception =>
            }
            tablesCreated = true
        }

        repository
    }

}
