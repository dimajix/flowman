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

import java.lang
import java.sql.Statement
import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.Column
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.catalog.TableChange
import com.dimajix.flowman.catalog.TableChange.ChangeStorageFormat
import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.execution.MergeClause
import com.dimajix.flowman.types.BinaryType
import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.CharType
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.ShortType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.TimestampType
import com.dimajix.flowman.types.VarcharType


object SqlServerDialect extends BaseDialect {
    // Special JDBC types in Microsoft SQL Server.
    // https://github.com/microsoft/mssql-jdbc/blob/v8.2.2/src/main/java/microsoft/sql/Types.java
    private object SpecificTypes {
        val GEOMETRY = -157
        val GEOGRAPHY = -158
    }

    private object Statements extends MsSqlServerStatements(this)
    private object Commands extends MsSqlServerCommands(this)

    override def canHandle(url : String): Boolean = url.toLowerCase(Locale.ROOT).startsWith("jdbc:sqlserver")

    override def getJdbcType(dt: FieldType): JdbcType = dt match {
        case TimestampType => JdbcType("DATETIME", java.sql.Types.TIMESTAMP)
        case StringType => JdbcType("NVARCHAR(MAX)", java.sql.Types.NVARCHAR)
        case v : CharType => JdbcType(s"NCHAR(${v.length})", java.sql.Types.NCHAR)
        case v : VarcharType => JdbcType(s"NVARCHAR(${v.length})", java.sql.Types.NVARCHAR)
        case BooleanType => JdbcType("BIT", java.sql.Types.BIT)
        case BinaryType => JdbcType("VARBINARY(MAX)", java.sql.Types.VARBINARY)
        case ShortType => JdbcType("SMALLINT", java.sql.Types.SMALLINT)
        case _ => super.getJdbcType(dt)
    }

    override def getFieldType(sqlType: Int, typeName:String, precision: Int, scale: Int, signed: Boolean): FieldType = {
        if (typeName.contains("datetimeoffset")) {
            // String is recommend by Microsoft SQL Server for datetimeoffset types in non-MS clients
            StringType
        }
        else {
            sqlType match {
                case java.sql.Types.SMALLINT => ShortType
                case java.sql.Types.REAL => FloatType
                case SpecificTypes.GEOMETRY | SpecificTypes.GEOGRAPHY => BinaryType
                case java.sql.Types.VARCHAR if precision <= 0 || precision >= 1073741823 => StringType
                case java.sql.Types.NVARCHAR if precision <= 0 || precision >= 1073741823 => StringType
                case _ => super.getFieldType(sqlType, typeName, precision, scale, signed)
            }
        }
    }

    /**
     * Returns true if the given table supports a specific table change
     * @param change
     * @return
     */
    override def supportsChange(table:TableIdentifier, change:TableChange) : Boolean = {
        change match {
            case _:ChangeStorageFormat => true
            case _:TableChange => super.supportsChange(table, change)
        }
    }

    /**
     * Returns true if the SQL database supports retrieval of the exact view definition
     *
     * @return
     */
    override def supportsExactViewRetrieval: Boolean = true

    /**
     * Returns true if a view definition can be changed
     * @return
     */
    override def supportsAlterView : Boolean = true

    override def statement : SqlStatements = Statements
    override def command : SqlCommands = Commands
}


class MsSqlServerStatements(dialect: BaseDialect) extends BaseStatements(dialect)  {
    /**
     * The SQL query for creating a new table
     *
     * @param table
     * @return
     */
    override def alterView(table: TableIdentifier, sql: String): String = {
        s"ALTER VIEW ${dialect.quote(table)} AS $sql"
    }

    override def getViewDefinition(table: TableIdentifier): String = {
        s"""
           |SELECT
           |    definition
           |FROM sys.sql_modules
           |WHERE object_id = OBJECT_ID(${dialect.literal(dialect.quote(table))})
           |""".stripMargin
    }

    override def firstRow(table: TableIdentifier, condition:String) : String = {
        if (condition.isEmpty)
            s"SELECT TOP 1 * FROM ${dialect.quote(table)}"
        else
            s"SELECT TOP 1 * FROM ${dialect.quote(table)} WHERE $condition"
    }

    // see https://docs.microsoft.com/en-us/sql/relational-databases/tables/add-columns-to-a-table-database-engine?view=sql-server-ver15
    override def addColumn(table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None): String = {
        val nullable = if (isNullable) "NULL" else "NOT NULL"
        val col = collation.map(c => s" COLLATE $c").getOrElse("")
        s"ALTER TABLE ${dialect.quote(table)} ADD ${dialect.quoteIdentifier(columnName)} $dataType$col $nullable"
    }

    // See https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-rename-transact-sql?view=sql-server-ver15
    override def renameColumn(table: TableIdentifier, columnName: String, newName: String): String = {
        s"EXEC sp_rename '${dialect.quote(table)}.${dialect.quoteIdentifier(columnName)}', ${dialect.quoteIdentifier(newName)}, 'COLUMN'"
    }

    override def updateColumnNullability(table: TableIdentifier, columnName: String, dataType:String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None): String = {
        val nullable = if (isNullable) "NULL" else "NOT NULL"
        val col = collation.map(c => s" COLLATE $c").getOrElse("")
        s"ALTER TABLE ${dialect.quote(table)} ALTER COLUMN ${dialect.quoteIdentifier(columnName)} $dataType$col $nullable"
    }

    override def dropIndex(table: TableIdentifier, indexName: String): String = {
        s"DROP INDEX ${dialect.quote(table)}.${dialect.quoteIdentifier(indexName)}"
    }

    override def merge(targetTable: TableIdentifier, targetAlias:String, targetSchema:Option[StructType], sourceAlias:String, sourceSchema:StructType, condition:Column, clauses:Seq[MergeClause]) : String = {
        val sql = super.merge(targetTable, targetAlias, targetSchema, sourceAlias, sourceSchema, condition, clauses)
        sql + ";\n" // Add semicolon for MS SQL Server
    }

    override def merge(targetTable: TableIdentifier, targetAlias:String, targetSchema:Option[StructType], sourceTable: TableIdentifier, sourceAlias:String, sourceSchema:StructType,  condition:Column, clauses:Seq[MergeClause]) : String = {
        val sql = super.merge(targetTable, targetAlias, targetSchema, sourceTable, sourceAlias, sourceSchema, condition, clauses)
        sql + ";\n" // Add semicolon for MS SQL Server
    }
}


class MsSqlServerCommands(dialect: BaseDialect) extends BaseCommands(dialect) {
    override def createTable(statement:Statement, table:TableDefinition) : Unit = {
        val tableSql = dialect.statement.createTable(table)
        val indexSql = table.indexes.map(idx => dialect.statement.createIndex(table.identifier, idx))
        statement.executeUpdate(tableSql)

        // Attach any comments

        // Optionally create CLUSTERED COLUMNSTORE INDEX
        table.storageFormat.map(_.toLowerCase(Locale.ROOT)) match {
            case Some("columnstore") =>
                statement.executeUpdate(s"CREATE CLUSTERED COLUMNSTORE INDEX columnstore_idx ON ${dialect.quote(table.identifier)}")
            case Some("rowstore") =>
            case Some(s) => throw new UnsupportedOperationException(s"Storage format '$s' not supported, only 'ROWSTORE' and 'COLUMNSTORE'")
            case None =>
        }

        // Create other indexes
        indexSql.foreach(statement.executeUpdate)
    }

    override def getJdbcSchema(statement:Statement, table:TableIdentifier) : Seq[JdbcField] = {
        // Get basic information
        val fields = super.getJdbcSchema(statement, table)

        // Query extended information
        val sql =
            s"""
              |SELECT
              |    c.name,
              |    c.collation_name
              |FROM sys.columns c
              |WHERE c.object_id = OBJECT_ID(${dialect.literal(dialect.quote(table))})
              |""".stripMargin
        val collations = queryKeyValue(statement, sql)
        val comments = getColumnComments(statement, table)

        // Merge base info and extra info
        fields.map { f =>
            val c1 = collations.get(f.name)
                .map(c => f.copy(collation=Option(c)))
                .getOrElse(f)
            comments.get(c1.name)
                .map(c => c1.copy(description=Option(c)))
                .getOrElse(c1)
        }
    }

    private def getColumnComments(statement:Statement, table:TableIdentifier) : Map[String,String] = {
        // Query extended information
        val sql =
            s"""
               |SELECT
               |    objname,
               |    TRY_CAST(value AS NVARCHAR(MAX)) AS value
               |FROM sys.fn_listextendedproperty ('MS_Description', 'SCHEMA', ${table.database.map(dialect.literal).getOrElse("schema_name()")}, 'TABLE', ${dialect.literal(table.table)}, 'column', null)
               |""".stripMargin
        queryKeyValue(statement, sql)
    }
    private def queryKeyValue(statement:Statement, sql:String) : Map[String,String] = {
        val rs = statement.executeQuery(sql)
        val values = mutable.Map[String,String]()
        try {
            while (rs.next()) {
                val name = rs.getString(1)
                val value = rs.getString(2)
                if (value != null)
                    values.put(name, value)
            }
        }
        finally {
            rs.close()
        }

        values.toMap
    }

    override def updateComment(statement:Statement, table: TableIdentifier, column:String, comment:Option[String]) : Unit = {
        // 1. Check if we currently do have a comment
        val sql =
        s"""
           |SELECT
           |    objname,
           |    value
           |FROM sys.fn_listextendedproperty ('MS_Description', 'SCHEMA', ${table.database.map(dialect.literal).getOrElse("schema_name()")}, 'TABLE', ${dialect.literal(table.table)}, 'column', ${dialect.literal(column)})
           |""".stripMargin
        if (queryKeyValue(statement, sql).nonEmpty) {
            comment match {
                case Some(c) =>
                    val sql = s"""
                        |exec sp_updateextendedproperty
                        |   'MS_Description', ${dialect.literal(c)},
                        |   'SCHEMA', ${table.database.map(dialect.literal).getOrElse("schema_name()")},
                        |   'TABLE', ${dialect.literal(table.table)},
                        |   'COLUMN', ${dialect.literal(column)}
                        |""".stripMargin
                    statement.executeUpdate(sql)
                case None =>
                    val sql = s"""
                        |exec sp_dropextendedproperty
                        |   'MS_Description',
                        |   'SCHEMA', ${table.database.map(dialect.literal).getOrElse("schema_name()")},
                        |   'TABLE', ${dialect.literal(table.table)},
                        |   'COLUMN', ${dialect.literal(column)}
                        |""".stripMargin
                    statement.executeUpdate(sql)
            }
        }
        else {
            comment match {
                case Some(c) =>
                    val sql = s"""
                        |exec sp_addextendedproperty
                        |   'MS_Description', ${dialect.literal(c)},
                        |   'SCHEMA', ${table.database.map(dialect.literal).getOrElse("schema_name()")},
                        |   'TABLE', ${dialect.literal(table.table)},
                        |   'COLUMN', ${dialect.literal(column)}
                        |""".stripMargin
                    statement.executeUpdate(sql)
                case None => // Nothing to do
            }
        }
    }

    override def getStorageFormat(statement:Statement, table:TableIdentifier) : Option[String] = {
        getColumnStoreIndex(statement, table) match {
            case Some(_) => Some("COLUMNSTORE")
            case None => Some("ROWSTORE")
        }
    }

    override def changeStorageFormat(statement: Statement, table: TableIdentifier, storageFormat: String): Unit = {
        // 1. Retrieve current storage format
        val current = getStorageFormat(statement, table)

        // 2. Conditionally create or drop CLUSTERED COLUMN STORE INDEX
        val desiredFormat = storageFormat.toLowerCase(Locale.ROOT)
        if (!current.exists(_.toLowerCase(Locale.ROOT) == desiredFormat)) {
            desiredFormat match {
                case "columnstore" =>
                    statement.executeUpdate(s"CREATE CLUSTERED COLUMNSTORE INDEX columnstore_idx ON ${dialect.quote(table)}")
                case "rowstore" =>
                    val indexName = getColumnStoreIndex(statement, table)
                    indexName.foreach(idx => statement.executeUpdate(s"DROP INDEX $idx ON ${dialect.quote(table)}"))
                case s => throw new UnsupportedOperationException(s"Storage format '$s' not supported, only 'ROWSTORE' and 'COLUMNSTORE'")
            }
        }
    }

    private def getColumnStoreIndex(statement: Statement, table: TableIdentifier) : Option[String] = {
        val sql =
            s"""
               |SELECT
               |    name,
               |    type_desc
               |FROM sys.indexes
               |WHERE object_id = OBJECT_ID(${dialect.literal(dialect.quote(table))})
               |AND type_desc = 'CLUSTERED COLUMNSTORE'
               |""".stripMargin
        val rs = statement.executeQuery(sql)
        var name:Option[String] = None
        try {
            while (rs.next()) {
                name = Option(rs.getString(1))
            }
        }
        finally {
            rs.close()
        }
        name
    }

    override def dropPrimaryKey(statement: Statement, table: TableIdentifier): Unit = {
        val meta = statement.getConnection.getMetaData
        val pkrs = meta.getPrimaryKeys(null, table.database.orNull, table.table)
        var name:String = ""
        while(pkrs.next()) {
            val pkname = pkrs.getString(6)
            if (pkname != null)
                name = pkname
        }
        pkrs.close()

        if (name.nonEmpty) {
            dropConstraint(statement, table, name)
            //dropIndex(statement, table, name)
        }
    }
}
