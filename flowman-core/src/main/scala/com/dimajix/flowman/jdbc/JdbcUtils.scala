/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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
import java.sql.DatabaseMetaData
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Statement
import java.util.Locale

import scala.collection.mutable
import scala.util.Try

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createConnectionFactory
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.savePartition
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{types => st}
import org.slf4j.LoggerFactory
import slick.jdbc.DerbyProfile
import slick.jdbc.H2Profile
import slick.jdbc.JdbcProfile
import slick.jdbc.MySQLProfile
import slick.jdbc.PostgresProfile
import slick.jdbc.SQLServerProfile
import slick.jdbc.SQLiteProfile

import com.dimajix.common.MapIgnoreCase
import com.dimajix.flowman.catalog.TableChange
import com.dimajix.flowman.catalog.TableChange.AddColumn
import com.dimajix.flowman.catalog.TableChange.CreateIndex
import com.dimajix.flowman.catalog.TableChange.CreatePrimaryKey
import com.dimajix.flowman.catalog.TableChange.DropColumn
import com.dimajix.flowman.catalog.TableChange.DropIndex
import com.dimajix.flowman.catalog.TableChange.DropPrimaryKey
import com.dimajix.flowman.catalog.TableChange.UpdateColumnComment
import com.dimajix.flowman.catalog.TableChange.UpdateColumnNullability
import com.dimajix.flowman.catalog.TableChange.UpdateColumnType
import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableIndex
import com.dimajix.flowman.catalog.TableType
import com.dimajix.flowman.execution.MergeClause
import com.dimajix.flowman.types.CharType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.VarcharType


case class JdbcField(
    name:String,
    typeName:String,
    dataType:Int,
    fieldSize:Int,
    fieldScale:Int,
    isSigned:Boolean,
    nullable:Boolean
)

class JdbcUtils
object JdbcUtils {
    private val logger = LoggerFactory.getLogger(classOf[JdbcUtils])

    /**
     * This method adjusts the schema of a JDBC target table to be compatible with an incoming Spark schema for
     * write operations. This will be used for intermediate tables.
     * @param tableSchema
     * @param dataSchema
     * @return
     */
    def createSchema(dataSchema:st.StructType, tableSchema:StructType) : StructType = {
        def combineFields(dataField:st.StructField, tableField:Field) : Field = {
            val ftype = dataField.dataType match {
                // Try to keep original types for Sparks generic String type
                case StringType =>
                    tableField.ftype match {
                        case t:VarcharType => t
                        case t:CharType => t
                        case _ => FieldType.of(dataField.dataType)
                    }
                // Use natural type for everything else
                case _ => FieldType.of(dataField.dataType)
            }

            tableField.copy(ftype=ftype, nullable=dataField.nullable)
        }
        val dataFields = MapIgnoreCase(dataSchema.fields.map(f => f.name -> f))
        val tableFields = tableSchema.fields.map { tgtField =>
            dataFields.get(tgtField.name)
                .map(srcField => combineFields(srcField, tgtField))
                .getOrElse(tgtField)
        }
        StructType(tableFields)
    }

    def queryTimeout(options: JDBCOptions) : Int = {
        // This is not very efficient, but in Spark 2.2 we cannot access parameters
        options.asProperties.getProperty("queryTimeout", "0").toInt
    }

    def createConnection(options: JDBCOptions) : Connection = {
        val factory = createConnectionFactory(options)
        factory()
    }

    def withTransaction[T](con:java.sql.Connection)(fn: => T) : T = {
        val oldMode = con.getAutoCommit
        con.setAutoCommit(false)
        try {
            val result = fn
            con.commit()
            result
        } catch {
            case ex:SQLException =>
                logger.error(s"SQL transaction failed, rolling back: ${ex.getMessage}")
                con.rollback()
                throw ex
        } finally {
            con.setAutoCommit(oldMode)
        }
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
     * Returns the table definition of a table
     * @param conn
     * @param table
     * @param options
     * @return
     */
    def getTable(conn: Connection, table:TableIdentifier, options: JDBCOptions) : TableDefinition = {
        val meta = conn.getMetaData
        val (realTable,realType) = resolveTable(meta, table)

        val currentSchema = getSchema(conn, table, options)
        val pk = getPrimaryKey(meta, realTable)
        val idxs = getIndexes(meta, realTable)
            // Remove primary key
            .filter { idx =>
                idx.normalize().columns != pk.map(_.toLowerCase(Locale.ROOT)).sorted
            }

        TableDefinition(table, realType, currentSchema.fields, primaryKey=pk, indexes=idxs)
    }

    private def getPrimaryKey(meta: DatabaseMetaData, table:TableIdentifier) : Seq[String] = {
        val pkrs = meta.getPrimaryKeys(null, table.database.orNull, table.table)
        val pk = mutable.ListBuffer[(Short,String)]()
        while(pkrs.next()) {
            val col = pkrs.getString(4)
            val seq = pkrs.getShort(5)
            // val name = pkrs.getString(6)
            pk.append((seq,col))
        }
        pkrs.close()
        pk.sortBy(_._1).map(_._2)
    }

    private def getIndexes(meta: DatabaseMetaData, table:TableIdentifier) : Seq[TableIndex] = {
        val idxrs = meta.getIndexInfo(null, table.database.orNull, table.table, false, true)
        val idxcols = mutable.ListBuffer[(String, String, Boolean)]()
        while(idxrs.next()) {
            val unique = !idxrs.getBoolean(4)
            val name = idxrs.getString(6) // May be null for statistics
            val col = idxrs.getString(9)
            idxcols.append((name, col, unique))
        }
        idxrs.close()

        idxcols.filter(_._1 != null)
            .groupBy(_._1).map { case(name,cols) =>
                TableIndex(name, cols.map(_._2), cols.foldLeft(false)(_ || _._3))
            }.toSeq
    }

    /**
     * Resolves the table name, even if upper/lower case does not match
     * @param conn
     * @param table
     * @return
     */
    private def resolveTable(meta: DatabaseMetaData, table:TableIdentifier) : (TableIdentifier,TableType) = {
        val tblrs = meta.getTables(null, table.database.orNull, null, Array("TABLE"))
        var tableName = table.table
        var tableType:TableType = TableType.UNKNOWN
        val db = table.database

        val TABLE = ".*TABLE.*".r
        val VIEW = ".*VIEW.*".r

        while(tblrs.next()) {
            val thisName = tblrs.getString(3)
            if (tableName.toLowerCase(Locale.ROOT) == thisName.toLowerCase(Locale.ROOT)) {
                tableName = thisName
                tableType = tblrs.getString(4) match {
                    case VIEW() => TableType.VIEW
                    case TABLE() => TableType.TABLE
                    case "GLOBAL TEMPORARY" => TableType.TABLE
                    case "LOCAL TEMPORARY" => TableType.TABLE
                    case _ => TableType.UNKNOWN
                }
            }
        }
        tblrs.close()

        (TableIdentifier(tableName, db), tableType)
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

    def getSchema(resultSet: ResultSet, dialect: SqlDialect) : StructType = {
        val schema = getJdbcSchema(resultSet)
        getSchema(schema, dialect)
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
            val rs = statement.executeQuery()
            try {
                getJdbcSchema(rs)
            }
            finally {
                rs.close()
            }
        }
    }

    /**
     * Takes a [[ResultSet]] and returns a list of [[JdbcField]] schema.
     *
     * @return A [[StructType]] giving the Catalyst schema.
     * @throws SQLException if the schema contains an unsupported type.
     */
    def getJdbcSchema(resultSet: ResultSet): Seq[JdbcField] = {
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
        val tableSql = dialect.statement.createTable(table)
        val indexSql = table.indexes.map(idx => dialect.statement.createIndex(table.identifier, idx))
        withStatement(conn, options) { statement =>
            statement.executeUpdate(tableSql)
            indexSql.foreach(statement.executeUpdate)
        }
    }

    /**
     * Drops an existing table using the given connection. Will throw an exception of the table does not exist.
     * @param conn
     * @param table
     * @param options
     */
    def dropTable(conn:Connection, table:TableIdentifier, options: JDBCOptions, ifExists:Boolean=false) : Unit = {
        withStatement(conn, options) { statement =>
            if (!ifExists || tableExists(conn, table, options)) {
                dropTable(statement, table, options)
            }
        }
    }
    def dropTable(statement:Statement, table:TableIdentifier, options: JDBCOptions) : Unit = {
        val dialect = SqlDialects.get(options.url)
        statement.executeUpdate(s"DROP TABLE ${dialect.quote(table)}")
    }

    /**
     * Truncates a table (i.e. removes all records, but keeps the table definition alive). Will throw an exception
     * if the table does not exist.
     * @param conn
     * @param table
     * @param options
     */
    def truncateTable(conn:Connection, table:TableIdentifier, options: JDBCOptions) : Unit = {
        withStatement(conn, options) { statement =>
            truncateTable(statement, table, options)
        }
    }
    def truncateTable(statement:Statement, table:TableIdentifier, options: JDBCOptions) : Unit = {
        val dialect = SqlDialects.get(options.url)
        statement.executeUpdate(s"TRUNCATE TABLE ${dialect.quote(table)}")
    }

    /**
     * Deletes individual records (representing a logical partition) via a predicate condition
     * @param statement
     * @param table
     * @param condition
     * @param options
     */
    def truncatePartition(statement:Statement, table:TableIdentifier, condition:String, options: JDBCOptions) : Unit = {
        val dialect = SqlDialects.get(options.url)
        statement.executeUpdate(s"DELETE FROM ${dialect.quote(table)} WHERE $condition")
    }

    /**
     * Inserts new records into an existing table from a different existing table
     * @param statement
     * @param targetTable
     * @param sourceTable
     * @param options
     */
    def appendTable(statement:Statement, targetTable:TableIdentifier, sourceTable:TableIdentifier, options: JDBCOptions) : Unit = {
        val dialect = SqlDialects.get(options.url)
        statement.executeUpdate(s"INSERT INTO ${dialect.quote(targetTable)}  SELECT * FROM ${dialect.quote(sourceTable)}")
    }

    /**
     * Perform an SQL MERGE operation withpout an intermediate staging table
     * @param target
     * @param targetAlias
     * @param targetSchema
     * @param source
     * @param sourceAlias
     * @param condition
     * @param clauses
     * @param options
     */
    def mergeTable(target:TableIdentifier,
                   targetAlias:String,
                   targetSchema:Option[org.apache.spark.sql.types.StructType],
                   source: DataFrame,
                   sourceAlias:String,
                   condition:Column,
                   clauses:Seq[MergeClause],
                   options: JDBCOptions) : Unit = {
        val url = options.url
        val dialect = SqlDialects.get(url)
        val sparkDialect = JdbcDialects.get(url)
        val quotedTarget = dialect.quote(target)
        val getConnection: () => Connection = createConnectionFactory(options)
        val sourceSchema = source.schema
        val batchSize = options.batchSize
        val isolationLevel = options.isolationLevel
        val insertStmt = dialect.statement.merge(target, targetAlias, targetSchema, sourceAlias, sourceSchema, condition, clauses)
        val repartitionedDF = options.numPartitions match {
            case Some(n) if n <= 0 => throw new IllegalArgumentException("Invalid number of partitions")
            case Some(n) if n < source.rdd.getNumPartitions => source.coalesce(n)
            case _ => source
        }
        repartitionedDF.rdd.foreachPartition { iterator => savePartition(
            getConnection, quotedTarget, iterator, sourceSchema, insertStmt, batchSize, sparkDialect, isolationLevel, options)
        }
    }

    /**
     * Perform an SQL MERGE operation from a source table into a target table
     * @param statement
     * @param target
     * @param targetAlias
     * @param targetSchema
     * @param source
     * @param sourceAlias
     * @param sourceSchema
     * @param condition
     * @param clauses
     * @param options
     */
    def mergeTable(
        statement:Statement,
        target:TableIdentifier,
        targetAlias:String,
        targetSchema:Option[org.apache.spark.sql.types.StructType],
        source: TableIdentifier,
        sourceAlias:String,
        sourceSchema:org.apache.spark.sql.types.StructType,
        condition:Column,
        clauses:Seq[MergeClause],
        options: JDBCOptions) : Unit = {
        val url = options.url
        val dialect = SqlDialects.get(url)
        val sql = dialect.statement.merge(target, targetAlias, targetSchema, source, sourceAlias, sourceSchema, condition, clauses)
        statement.executeUpdate(sql)
    }

    /**
     * Adds an index to an existing table
     * @param conn
     * @param table
     * @param index
     * @param options
     */
    def createIndex(conn:Connection, table:TableIdentifier, index:TableIndex, options: JDBCOptions) : Unit = {
        val dialect = SqlDialects.get(options.url)
        val indexSql = dialect.statement.createIndex(table, index)
        withStatement(conn, options) { statement =>
            statement.executeUpdate(indexSql)
        }
    }

    /**
     * Drops an index from an existing table
     * @param conn
     * @param indexName
     * @param options
     */
    def dropIndex(conn:Connection, table:TableIdentifier, indexName:String, options: JDBCOptions) : Unit = {
        val dialect = SqlDialects.get(options.url)
        val indexSql = dialect.statement.dropIndex(table, indexName)
        withStatement(conn, options) { statement =>
            statement.executeUpdate(indexSql)
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
                logger.info(s"Dropping column ${a.column} from JDBC table $table")
                currentFields.remove(a.column.toLowerCase(Locale.ROOT))
                Some(statements.deleteColumn(table, a.column))
            case a:AddColumn =>
                val dataType = dialect.getJdbcType(a.column.ftype)
                logger.info(s"Adding column ${a.column.name} with type ${dataType.databaseTypeDefinition} (${a.column.ftype.sqlType}) to JDBC table $table")
                currentFields.put(a.column.name.toLowerCase(Locale.ROOT), JdbcField(a.column.name, dataType.databaseTypeDefinition, 0, 0, 0, false, a.column.nullable))
                Some(statements.addColumn(table, a.column.name, dataType.databaseTypeDefinition, a.column.nullable))
            case u:UpdateColumnType =>
                val current = currentFields(u.column.toLowerCase(Locale.ROOT))
                val dataType = dialect.getJdbcType(u.dataType)
                logger.info(s"Changing column ${u.column} type from ${current.typeName} to ${dataType.databaseTypeDefinition} (${u.dataType.sqlType}) in JDBC table $table")
                currentFields.put(u.column.toLowerCase(Locale.ROOT), current.copy(typeName=dataType.databaseTypeDefinition))
                Some(statements.updateColumnType(table, u.column, dataType.databaseTypeDefinition))
            case u:UpdateColumnNullability =>
                logger.info(s"Updating nullability of column ${u.column} to ${u.nullable} in JDBC table $table")
                val current = currentFields(u.column.toLowerCase(Locale.ROOT))
                currentFields.put(u.column.toLowerCase(Locale.ROOT), current.copy(nullable=u.nullable))
                Some(statements.updateColumnNullability(table, u.column, current.typeName, u.nullable))
            case u:UpdateColumnComment =>
                logger.info(s"Updating comment of column ${u.column} in JDBC table $table")
                None
            case idx:CreateIndex =>
                logger.info(s"Adding index ${idx.name} to JDBC table $table on columns ${idx.columns.mkString(",")}")
                Some(statements.createIndex(table, TableIndex(idx.name, idx.columns, idx.unique)))
            case idx:DropIndex =>
                logger.info(s"Dropping index ${idx.name} from JDBC table $table")
                Some(statements.dropIndex(table, idx.name))
            case pk:CreatePrimaryKey =>
                logger.info(s"Creating primary key for JDBC table $table on columns ${pk.columns.mkString(",")}")
                Some(statements.addPrimaryKey(table, pk.columns))
            case pk:DropPrimaryKey =>
                logger.info(s"Removing primary key from JDBC table $table}")
                Some(statements.dropPrimaryKey(table))
            case chg:TableChange => throw new SQLException(s"Unsupported table change $chg for JDBC table $table")
        }

        withStatement(conn, options) { statement =>
            sqls.foreach { sql =>
                statement.executeUpdate(sql)
            }
        }
    }

    def getProfile(driver:String) : JdbcProfile = {
        val derbyPattern = """.*\.derby\..*""".r
        val sqlitePattern = """.*\.sqlite\..*""".r
        val h2Pattern = """.*\.h2\..*""".r
        val mariadbPattern = """.*\.mariadb\..*""".r
        val mysqlPattern = """.*\.mysql\..*""".r
        val postgresqlPattern = """.*\.postgresql\..*""".r
        val sqlserverPattern = """.*\.sqlserver\..*""".r
        driver match {
            case derbyPattern() => DerbyProfile
            case sqlitePattern() => SQLiteProfile
            case h2Pattern() => H2Profile
            case mysqlPattern() => MySQLProfile
            case mariadbPattern() => MySQLProfile
            case postgresqlPattern() => PostgresProfile
            case sqlserverPattern() => SQLServerProfile
            case _ => throw new UnsupportedOperationException(s"Database with driver ${driver} is not supported")
        }
    }
}
