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

import java.sql.SQLRecoverableException

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.slf4j.LoggerFactory
import scalikejdbc.ConnectionPool
import scalikejdbc.DB
import scalikejdbc.DBSession

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.ConnectionIdentifier


class JdbcLoggedRunner extends AbstractRunner {
    private val logger = LoggerFactory.getLogger(classOf[JdbcLoggedRunner])

    @JsonProperty(value="connection", required=true) private[spec] var _connection:String = _
    @JsonProperty(value="retries", required=false) private[spec] var _retries:String = "3"
    @JsonProperty(value="timeout", required=false) private[spec] var _timeout:String = "1000"

    def connection(implicit context: Context) : ConnectionIdentifier = ConnectionIdentifier.parse(context.evaluate(_connection))
    def retries(implicit context: Context) : Int = context.evaluate(_retries).toInt
    def timeout(implicit context: Context) : Int = context.evaluate(_timeout).toInt

    protected override def check(context:Context) = false

    protected override def start(context:Context) : Object = null

    protected override def success(context: Context, token:Object) : Unit = {}

    protected override def failure(context: Context, token:Object) : Unit = {}

    protected override def aborted(context: Context, token:Object) : Unit = {}

    protected override def skipped(context: Context, token:Object) : Unit = {}


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
                DB(conn) autoCommit { session: DBSession => query(session) }
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
