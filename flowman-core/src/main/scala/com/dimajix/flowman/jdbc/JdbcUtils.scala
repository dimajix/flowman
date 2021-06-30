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
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Statement
import java.util.Locale

import scala.collection.mutable
import scala.util.Try

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import com.dimajix.flowman.catalog.TableChange
import TableChange.AddColumn
import TableChange.DropColumn
import TableChange.UpdateColumnComment
import TableChange.UpdateColumnNullability
import TableChange.UpdateColumnType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.StructType


case class JdbcField(
    name:String,
    typeName:String,
    dataType:Int,
    fieldSize:Int,
    fieldScale:Int,
    isSigned:Boolean,
    nullable:Boolean
)

object JdbcUtils {
    def queryTimeout(options: JDBCOptions) : Int = {
        // This is not very efficient, but in Spark 2.2 we cannot access parameters
        options.asProperties.getProperty("queryTimeout", "0").toInt
    }

    def createConnection(options: JDBCOptions) : Connection = {
        val factory = org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createConnectionFactory(options)
        factory()
    }

    def withStatement[T](conn:Connection, options: JDBCOptions)(fn:Statement => T) : T = {
        val statement = conn.createStatement()
        try {
            statement.setQueryTimeout(queryTimeout(options))
            fn(statement)
        } finally {
            statement.close()
        }
    }
    def withStatement[T](conn:Connection, query:String, options: JDBCOptions)(fn:PreparedStatement => T) : T = {
        val statement = conn.prepareStatement(query)
        try {
            statement.setQueryTimeout(queryTimeout(options))
            fn(statement)
        } finally {
            statement.close()
        }
    }

    /**
     * Checks the existence of a specific table using the given [[Connection]].
     * @param conn
     * @param table
     * @param options
     * @return
     */
    def tableExists(conn:Connection, table:TableIdentifier, options: JDBCOptions) : Boolean = {
        val dialect = SqlDialects.get(options.url)

        // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
        // SQL database systems using JDBC meta data calls, considering "table" could also include
        // the database name. Query used to find table exists can be overridden by the dialects.
        Try {
            withStatement(conn, dialect.statement.tableExists(table), options) { stmt =>
                stmt.executeQuery()
            }
        }.isSuccess
    }

    /**
     * Returns an empty result from the given table using the given [[Connection]]. This is useful for retrieving
     * table meta data such as column names and data types.
     * @param conn
     * @param table
     * @param options
     * @return
     */
    def emptyResult(conn:Connection, table:TableIdentifier, condition:String, options: JDBCOptions) : Boolean = {
        val dialect = SqlDialects.get(options.url)
        val sql = dialect.statement.firstRow(table, condition)
        withStatement(conn, options) { statement =>
            val result = statement.executeQuery(sql)
            try {
                !result.next()
            }
            finally {
                result.close()
            }
        }
    }

    /**
     * Returns the schema if the table already exists in the JDBC database.
     */
    def getSchema(conn: Connection, table:TableIdentifier, options: JDBCOptions): StructType = {
        val jdbcFields = getJdbcSchema(conn, table, options)

        val dialect = SqlDialects.get(options.url)
        getSchema(jdbcFields, dialect)
    }

    /**
     * Converts a given list of [[JdbcField]] to a Flowman [[StructType]]
     * @param jdbcFields
     * @param dialect
     * @return
     */
    def getSchema(jdbcFields:Seq[JdbcField], dialect: SqlDialect) : StructType = {
        val fields = jdbcFields.map { field =>
            val columnType = dialect.getFieldType(field.dataType, field.typeName, field.fieldSize, field.fieldScale, field.isSigned)
            Field(field.name, columnType, field.nullable)
        }

        StructType(fields)
    }

    /**
     * Returns the list of [[JdbcField]] definitions containing the deatiled JDBC schema of the specified table.
     * @param conn
     * @param table
     * @param options
     * @return
     */
    def getJdbcSchema(conn: Connection, table:TableIdentifier, options: JDBCOptions) : Seq[JdbcField] = {
        val dialect = SqlDialects.get(options.url)

        withStatement(conn, dialect.statement.schema(table), options) { statement =>
            getJdbcSchemaImpl(statement.executeQuery())
        }
    }

    /**
     * Takes a [[ResultSet]] and returns a list of [[JdbcField]] schema.
     *
     * @return A [[StructType]] giving the Catalyst schema.
     * @throws SQLException if the schema contains an unsupported type.
     */
    private def getJdbcSchemaImpl(resultSet: ResultSet): Seq[JdbcField] = {
        val rsmd = resultSet.getMetaData
        val ncols = rsmd.getColumnCount
        val fields = new Array[JdbcField](ncols)
        var i = 0
        while (i < ncols) {
            val columnName = rsmd.getColumnLabel(i + 1)
            val dataType = rsmd.getColumnType(i + 1)
            val typeName = rsmd.getColumnTypeName(i + 1)
            val fieldSize = rsmd.getPrecision(i + 1)
            val fieldScale = rsmd.getScale(i + 1)
            val isSigned = {
                try {
                    rsmd.isSigned(i + 1)
                } catch {
                    // Workaround for HIVE-14684:
                    case e: SQLException if
                        e.getMessage == "Method not supported" &&
                            rsmd.getClass.getName == "org.apache.hive.jdbc.HiveResultSetMetaData" => true
                }
            }
            val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
            fields(i) = JdbcField(columnName, typeName, dataType, fieldSize, fieldScale, isSigned, nullable)
            i = i + 1
        }
        fields
    }

    /**
     * Creates a new table using the given connection and [[TableDefinition]]. Will throw an exception if the
     * table already exists.
     * @param conn
     * @param table
     * @param options
     */
    def createTable(conn:Connection, table:TableDefinition, options: JDBCOptions) : Unit = {
        val dialect = SqlDialects.get(options.url)
        val sql = dialect.statement.create(table)
        withStatement(conn, options) { statement =>
            statement.executeUpdate(sql)
        }
    }

    /**
     * Drops an existing table using the given connection. Will throw an exception of the table does not exist.
     * @param conn
     * @param table
     * @param options
     */
    def dropTable(conn:Connection, table:TableIdentifier, options: JDBCOptions) : Unit = {
        val dialect = SqlDialects.get(options.url)
        withStatement(conn, options) { statement =>
            statement.executeUpdate(s"DROP TABLE ${dialect.quote(table)}")
        }
    }

    /**
     * Truncates a table (i.e. removes all records, but keeps the table definition alive). Will throw an exception
     * if the table does not exist.
     * @param conn
     * @param table
     * @param options
     */
    def truncateTable(conn:Connection, table:TableIdentifier, options: JDBCOptions) : Unit = {
        val dialect = SqlDialects.get(options.url)
        withStatement(conn, options) { statement =>
            statement.executeUpdate(s"TRUNCATE TABLE ${dialect.quote(table)}")
        }
    }

    /**
     * Applies a list of [[TableChange]] to an existing table. Will throw an exception if one of the operations
     * is not supported or if the table does not exist.
     * @param conn
     * @param table
     * @param changes
     * @param options
     */
    def alterTable(conn:Connection, table:TableIdentifier, changes:Seq[TableChange], options: JDBCOptions) : Unit = {
        val dialect = SqlDialects.get(options.url)
        val statements = dialect.statement

        // Get current schema, so we can lookup existing types etc
        val currentSchema = getJdbcSchema(conn, table, options)
        val currentFields = mutable.Map(currentSchema.map(f => (f.name.toLowerCase(Locale.ROOT), f)):_*)

        val sqls = changes.flatMap {
            case a:DropColumn =>
                currentFields.remove(a.column.toLowerCase(Locale.ROOT))
                Some(statements.deleteColumn(table, a.column))
            case a:AddColumn =>
                val dataType = dialect.getJdbcType(a.column.ftype)
                currentFields.put(a.column.name.toLowerCase(Locale.ROOT), JdbcField(a.column.name, dataType.databaseTypeDefinition, 0, 0, 0, false, a.column.nullable))
                Some(statements.addColumn(table, a.column.name, dataType.databaseTypeDefinition, a.column.nullable))
            case u:UpdateColumnType =>
                val current = currentFields(u.column.toLowerCase(Locale.ROOT))
                val dataType = dialect.getJdbcType(u.dataType)
                currentFields.put(u.column.toLowerCase(Locale.ROOT), current.copy(typeName=dataType.databaseTypeDefinition))
                Some(statements.updateColumnType(table, u.column, dataType.databaseTypeDefinition))
            case u:UpdateColumnNullability =>
                val current = currentFields(u.column.toLowerCase(Locale.ROOT))
                currentFields.put(u.column.toLowerCase(Locale.ROOT), current.copy(nullable=u.nullable))
                Some(statements.updateColumnNullability(table, u.column, current.typeName, u.nullable))
            case _:UpdateColumnComment =>
                None
            case chg:TableChange => throw new SQLException(s"Unsupported TableChange $chg")
        }

        withStatement(conn, options) { statement =>
            sqls.foreach { sql =>
                statement.executeUpdate(sql)
            }
        }
    }
}
