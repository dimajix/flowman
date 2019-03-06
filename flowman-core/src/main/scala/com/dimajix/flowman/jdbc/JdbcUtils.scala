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

package com.dimajix.flowman.jdbc

import java.sql.Connection

import scala.util.Try

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions


object JdbcUtils {
    def createConnection(options: JDBCOptions) : Connection = {
        val factory = org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createConnectionFactory(options)
        factory()
    }

    def tableExists(conn:Connection, table:TableIdentifier, options: JDBCOptions) : Boolean = {
        val dialect = SqlDialects.get(options.url)

        // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
        // SQL database systems using JDBC meta data calls, considering "table" could also include
        // the database name. Query used to find table exists can be overridden by the dialects.
        Try {
            val statement = conn.prepareStatement(dialect.statement.tableExists(table))
            try {
                statement.setQueryTimeout(options.queryTimeout)
                statement.executeQuery()
            } finally {
                statement.close()
            }
        }.isSuccess
    }

    def emptyResult(conn:Connection, table:TableIdentifier, condition:String, options: JDBCOptions) : Boolean = {
        val dialect = SqlDialects.get(options.url)
        val sql = dialect.statement.firstRow(table, condition)
        val statement = conn.createStatement
        try {
            statement.setQueryTimeout(options.queryTimeout)
            val result = statement.executeQuery(sql)
            try {
                !result.next()
            }
            finally {
                result.close()
            }
        } finally {
            statement.close()
        }
    }

    def createTable(conn:Connection, table:TableDefinition, options: JDBCOptions) : Unit = {
        val dialect = SqlDialects.get(options.url)
        val sql = dialect.statement.create(table)
        val statement = conn.createStatement
        try {
            statement.setQueryTimeout(options.queryTimeout)
            statement.executeUpdate(sql)
        } finally {
            statement.close()
        }
    }

    def dropTable(conn:Connection, table:TableIdentifier, options: JDBCOptions) : Unit = {
        val dialect = SqlDialects.get(options.url)
        val statement = conn.createStatement
        try {
            statement.setQueryTimeout(options.queryTimeout)
            statement.executeUpdate(s"DROP TABLE ${dialect.quote(table)}")
        } finally {
            statement.close()
        }
    }

    def truncateTable(conn:Connection, table:TableIdentifier, options: JDBCOptions) : Unit = {
        val dialect = SqlDialects.get(options.url)
        val statement = conn.createStatement
        try {
            statement.setQueryTimeout(options.queryTimeout)
            statement.executeUpdate(s"TRUNCATE TABLE ${dialect.quote(table)}")
        } finally {
            statement.close()
        }
    }
}
