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
import scalikejdbc.ConnectionPool
import scalikejdbc.DB
import scalikejdbc.DBSession
import scalikejdbc.SQLSyntaxSupport
import scalikejdbc.SubQuery
import scalikejdbc.insert
import scalikejdbc.scalikejdbcSQLInterpolationImplicitDef
import scalikejdbc.select
import scalikejdbc.update
import scalikejdbc.withSQL

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.task.Job


object JdbcLoggedRunner {
    private val STATUS_SUCCESS = "success"
    private val STATUS_FAILED = "failed"
    private val STATUS_RUNNING = "running"
    private val STATUS_SKIPPED = "skipped"
    private val STATUS_KILLED = "killed"

    private object Run extends SQLSyntaxSupport[Run] {
        override val tableName = "run"
        override val columnNames = Seq("id", "namespace", "project", "job", "args_hash", "start_ts", "end_ts", "status")
    }
    private case class Run(id:Long, namespace: String, project:String, job:String, args_hash:String, start_ts:Timestamp, end_ts:Timestamp, status:String)
}


class JdbcLoggedRunner extends AbstractRunner {
    import JdbcLoggedRunner._

    private val logger = LoggerFactory.getLogger(classOf[JdbcLoggedRunner])

    @JsonProperty(value="connection", required=true) private[spec] var _connection:String = _
    @JsonProperty(value="retries", required=false) private[spec] var _retries:String = "3"
    @JsonProperty(value="timeout", required=false) private[spec] var _timeout:String = "1000"

    def connection(implicit context: Context) : ConnectionIdentifier = ConnectionIdentifier.parse(context.evaluate(_connection))
    def retries(implicit context: Context) : Int = context.evaluate(_retries).toInt
    def timeout(implicit context: Context) : Int = context.evaluate(_timeout).toInt

    protected override def check(context:Context, job:Job, args:Map[String,String]) : Boolean = {
        import scalikejdbc.SQLSyntax.max

        val run =  Run(
            0,
            Option(context.namespace.name).getOrElse(""),
            Option(context.project.name).getOrElse(""),
            job.name,
            hashArgs(job, args),
            null,
            null,
            null
        )
        logger.info(s"Checking last state for job ${run.namespace}/${run.project}/${run.job} in state database")
        val result =
            withSession(context) { implicit session =>
                val r = Run.syntax("r")
                val sq = SubQuery.syntax("sq").include(r)
                withSQL {
                    select(r.status)
                        .from(Run as r)
                        .where.in(r.id,
                        select(sqls"${max(sq(r).id)} as id")
                            .from(Run as r)
                            .where.eq(r.namespace, run.namespace)
                            .and.eq(r.project, run.project)
                            .and.eq(r.job, run.job)
                            .and.eq(r.args_hash, run.args_hash)
                            .and.not.eq(r.status, STATUS_SKIPPED)
                    )
                }.map(rs => rs.get[String]("status")).first.apply()
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
            Option(context.namespace.name).getOrElse(""),
            Option(context.project.name).getOrElse(""),
            job.name,
            hashArgs(job, args),
            now,
            null,
            STATUS_RUNNING
        )

        logger.info(s"Writing start marker for job ${run.namespace}/${run.project}/${run.job} into state database")
        val runId =
            withSession(context) { implicit session =>
                withSQL {
                    insert.into(Run).namedValues(
                        Run.column.namespace -> run.namespace,
                        Run.column.project -> run.project,
                        Run.column.job -> run.job,
                        Run.column.start_ts -> run.start_ts,
                        Run.column.status -> run.status
                    )
                }.updateAndReturnGeneratedKey.apply()
            }

        run.copy(id = runId)
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
    private def setState(context: Context, run: Run, status:String) = {
        logger.info(s"Mark last run of job ${run.namespace}/${run.project}/${run.job} as $status in state database")
        val now = new Timestamp(Clock.systemDefaultZone().instant().toEpochMilli)

        withSession(context) { implicit session =>
            withSQL {
                update(Run).set(
                    Run.column.end_ts -> now,
                    Run.column.status -> status
                )
                .where.eq(Run.column.id, run.id)
            }.update.apply()
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
    private def withSession[T](context: Context)(query: DBSession => T) : T = {
        implicit val icontext = context

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
            scalikejdbc.using(connect(context).borrow()) { conn =>
                DB(conn) autoCommit query
            }
        }
    }

    /**
      * Connects to a JDBC source
      *
      * @param context
      * @return
      */
    private def connect(context: Context) : ConnectionPool = {
        implicit val icontext = context

        // Get Connection
        val connection = context.getConnection(this.connection)
        if (connection == null)
            throw new NoSuchElementException(s"Connection '${this.connection}' not defined.")

        if (!ConnectionPool.isInitialized(connection)) {
            val url = connection.url
            logger.info(s"Connecting via JDBC to ${url}")
            DriverRegistry.register(connection.driver)

            ConnectionPool.add(connection, url, connection.username, connection.password)
        }
        ConnectionPool.get(connection)
    }
}
