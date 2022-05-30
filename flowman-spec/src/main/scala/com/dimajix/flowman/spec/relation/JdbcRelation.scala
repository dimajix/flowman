/*
 * Copyright 2022 Kaya Kupferschmidt
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
package com.dimajix.flowman.spec.relation

import java.sql.Statement

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableType
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.jdbc.JdbcUtils
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.spec.connection.JdbcConnection


abstract class JdbcRelation(
    connection: Reference[Connection],
    properties: Map[String,String] = Map.empty
) extends BaseRelation with PartitionedRelation {
    protected val logger: Logger = LoggerFactory.getLogger(getClass)

    protected def dropTableOrView(execution: Execution, table:TableIdentifier, ifExists: Boolean) : Unit = {
        require(execution != null)

        logger.info(s"Destroying JDBC relation '$identifier', this will drop JDBC table/view $table")
        withConnection{ (con,options) =>
            JdbcUtils.dropTableOrView(con, table, options, ifExists)
            provides.foreach(execution.refreshResource)
        }
    }

    protected def createConnectionProperties() : (String,Map[String,String]) = {
        val connection = this.connection.value.asInstanceOf[JdbcConnection]
        val props = mutable.Map[String,String]()
        props.put(JDBCOptions.JDBC_URL, connection.url)
        props.put(JDBCOptions.JDBC_DRIVER_CLASS, connection.driver)
        connection.username.foreach(props.put("user", _))
        connection.password.foreach(props.put("password", _))

        connection.properties.foreach(kv => props.put(kv._1, kv._2))
        properties.foreach(kv => props.put(kv._1, kv._2))

        (connection.url,props.toMap)
    }

    protected def withConnection[T](fn:(java.sql.Connection,JDBCOptions) => T) : T = {
        val (url,props) = createConnectionProperties()
        logger.debug(s"Connecting to jdbc source at $url")

        val options = new JDBCOptions(props)
        val conn = try {
            JdbcUtils.createConnection(options)
        } catch {
            case NonFatal(e) =>
                logger.error(s"Error connecting to jdbc source at $url: ${e.getMessage}")
                throw e
        }

        try {
            fn(conn, options)
        }
        finally {
            conn.close()
        }
    }

    protected def withTransaction[T](con:java.sql.Connection)(fn: => T) : T = {
        JdbcUtils.withTransaction(con)(fn)
    }

    protected def withStatement[T](fn:(Statement,JDBCOptions) => T) : T = {
        withConnection { (con, options) =>
            withStatement(con,options)(fn)
        }
    }

    protected def withStatement[T](con:java.sql.Connection,options:JDBCOptions)(fn:(Statement,JDBCOptions) => T) : T = {
        val statement = con.createStatement()
        try {
            statement.setQueryTimeout(JdbcUtils.queryTimeout(options))
            fn(statement, options)
        }
        finally {
            statement.close()
        }
    }

}
