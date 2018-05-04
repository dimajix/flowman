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

import com.fasterxml.jackson.annotation.JsonProperty
import javax.xml.bind.DatatypeConverter
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.slf4j.LoggerFactory
import org.squeryl.AbstractSession
import org.squeryl.KeyedEntity
import org.squeryl.PrimitiveTypeMode
import org.squeryl.Schema
import org.squeryl.Session
import org.squeryl.adapters.DerbyAdapter
import org.squeryl.adapters.MySQLAdapter
import org.squeryl.adapters.PostgreSqlAdapter
import org.squeryl.internals.DatabaseAdapter

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


private object JdbcLoggedRunnerModel extends PrimitiveTypeMode {
    case class Run (
           id:Long,
           namespace: String,
           project:String,
           job:String,
           args_hash:String,
           start_ts:Timestamp,
           end_ts:Timestamp,
           status:String
       ) extends KeyedEntity[Long] {
    }

    object Library extends Schema {
        val runs = table[Run]("JOB_RUN")

        on(runs)(b => declare(
            b.id is(primaryKey, autoIncremented),
            b.namespace defaultsTo(""),
            b.project defaultsTo(""),
            b.job defaultsTo(""),
            b.args_hash defaultsTo(""),
            b.start_ts is(),
            b.end_ts is(),
            b.status defaultsTo(""),

            columns(b.namespace, b.project, b.job, b.args_hash, b.status) are (indexed)
        ))

        def getStatus(run:Run) : Option[String] = {
            from(runs)(r =>
                where(r.id in
                    from(runs)(r2 =>
                        where(r2.namespace === run.namespace
                            and r2.project === run.project
                            and r2.job === run.job
                            and r2.args_hash === run.args_hash
                            and r2.status <> JdbcLoggedRunner.STATUS_SKIPPED
                        )
                        compute max(r2.id)
                    )
                )
                select r.status
            ).singleOption
        }

        def insertRun(run:Run) : Run = {
            transaction(Session.currentSession) {
                Library.runs.insert(run)
            }
        }
        def setState(run:Run) : Int = {
            transaction(Session.currentSession) {
                update(Library.runs)(r =>
                    where(r.id === run.id)
                        .set(
                            r.end_ts := run.end_ts,
                            r.status := run.status
                        )
                )
            }
        }
    }
}

class JdbcLoggedRunner extends AbstractRunner {
    import JdbcLoggedRunner._
    import JdbcLoggedRunnerModel._

    private val logger = LoggerFactory.getLogger(classOf[JdbcLoggedRunner])

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
            withSession(context) {
                Library.getStatus(run)
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
        withSession(context) {
            Library.insertRun(run)
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
        withSession(context) {
            Library.setState(run.copy(end_ts = now, status=status))
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
    private def withSession[T](context: Context)(query: => T) : T = {
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
            val session = newSession(icontext)
            try {
                using(session) {
                    query
                }
            }
            finally {
                session.close
            }
        }
    }

    private var tablesCreated:Boolean = false

    /**
      * Connects to a JDBC source
      *
      * @param context
      * @return
      */
    private def connect(context: Context, connection:Connection) : java.sql.Connection = {
        implicit val icontext = context

        val url = connection.url
        logger.info(s"Connecting via JDBC to $url")

        DriverRegistry.register(connection.driver)

        java.sql.DriverManager.getConnection(connection.url)
    }

    private def newSession(context:Context) : Session = {
        implicit val icontext = context

        // Get Connection
        val connection = context.getConnection(this.connection)
        if (connection == null)
            throw new NoSuchElementException(s"Connection '${this.connection}' not defined.")

        val derbyPattern = """.*\.derby\..*""".r
        val mysqlPattern = """.*\.mysql\..*""".r
        val postgresqlPattern = """.*\.postgresql\..*""".r
        val adapter = connection.driver match {
            case derbyPattern() => new DerbyAdapter
            case mysqlPattern() => new MySQLAdapter
            case postgresqlPattern() => new PostgreSqlAdapter
            case _ => throw new UnsupportedOperationException(s"Database with driver ${connection.driver} is not supported")
        }

        val session = Session.create(
            connect(context, connection),
            adapter)

        // Create Database if not exists
        if (!tablesCreated) {
            using(session) {
                try {
                    Library.create
                }
                catch {
                    case _:Exception =>
                }
            }
            tablesCreated = true
        }

        session
    }
}
