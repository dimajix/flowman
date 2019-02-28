package com.dimajix.flowman.jdbc

import java.sql.Connection

import scala.util.Try

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.JdbcType

import com.dimajix.flowman.types.FieldType


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
