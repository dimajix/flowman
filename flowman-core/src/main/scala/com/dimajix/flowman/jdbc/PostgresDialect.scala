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

import java.sql.Statement
import java.sql.Types
import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.jdbc.JdbcType

import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.types.BinaryType
import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.ByteType
import com.dimajix.flowman.types.DecimalType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.ShortType
import com.dimajix.flowman.types.StringType


object PostgresDialect extends BaseDialect {
    private object Statements extends PostgresStatements(this)
    private object Expressions extends PostgresExpressions(this)
    private object Commands extends PostgresCommands(this)

    override def canHandle(url: String): Boolean = url.startsWith("jdbc:postgresql")

    override def getFieldType(sqlType: Int, typeName:String, precision: Int, scale: Int, signed: Boolean): FieldType = {
        if (sqlType == Types.REAL) {
            FloatType
        } else if (sqlType == Types.SMALLINT) {
            ShortType
        } else if (sqlType == Types.BIT && typeName == "bit" && precision != 1) {
            BinaryType
        } else if (sqlType == Types.DOUBLE && typeName == "money") {
            // money type seems to be broken but one workaround is to handle it as string.
            // See SPARK-34333 and https://github.com/pgjdbc/pgjdbc/issues/100
            StringType
        } else if (sqlType == Types.OTHER) {
            StringType
        } else {
            super.getFieldType(sqlType, typeName, precision, scale, signed)
        }
    }

    override def getJdbcType(dt: FieldType): JdbcType = dt match {
        case StringType => JdbcType("TEXT", Types.CHAR)
        case BinaryType => JdbcType("BYTEA", Types.BINARY)
        case BooleanType => JdbcType("BOOLEAN", Types.BOOLEAN)
        case FloatType => JdbcType("FLOAT4", Types.FLOAT)
        case DoubleType => JdbcType("FLOAT8", Types.DOUBLE)
        case ShortType | ByteType => JdbcType("SMALLINT", Types.SMALLINT)
        case t: DecimalType => JdbcType(s"NUMERIC(${t.precision},${t.scale})", java.sql.Types.NUMERIC)
        case _ => super.getJdbcType(dt)
    }

    /**
     * Returns true if a view definition can be changed
     * @return
     */
    override def supportsAlterView : Boolean = true

    override def statement : SqlStatements = Statements
    override def expr : SqlExpressions = Expressions
    override def command : SqlCommands = Commands
}


class PostgresExpressions(dialect: BaseDialect) extends BaseExpressions(dialect) {
    override def collate(charset:Option[String], collation:Option[String]) : String = {
        collation.map(c => s""" COLLATE "$c"""").getOrElse("")
    }
}

class PostgresStatements(dialect: BaseDialect) extends BaseStatements(dialect)  {
    /**
     * Get the SQL query that should be used to find if the given table exists. Dialects can
     * override this method to return a query that works best in a particular database.
     *
     * @param table The name of the table.
     * @return The SQL query to use for checking the table.
     */
    override def tableExists(table: TableIdentifier): String =
        s"SELECT 1 FROM ${dialect.quote(table)} LIMIT 1"


    override def getViewDefinition(table: TableIdentifier): String = {
        s"""
           |SELECT
           |    view_definition
           |FROM information_schema.views
           |WHERE table_catalog = current_catalog
           |  AND table_schema = ${table.space.headOption.map(s => dialect.literal(s.toUpperCase(Locale.ROOT))).getOrElse("current_schema")}
           |  AND table_name = ${dialect.literal(table.table.toUpperCase(Locale.ROOT))}
           |""".stripMargin
    }

    override def updateColumnType(table: TableIdentifier, columnName: String, newDataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): String = {
        val col = dialect.expr.collate(charset, collation)
        s"ALTER TABLE ${dialect.quote(table)} ALTER COLUMN ${dialect.quoteIdentifier(columnName)} TYPE $newDataType$col"
    }

    override def updateColumnNullability(table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): String = {
        val nullable = if (isNullable) "DROP NOT NULL" else "SET NOT NULL"
        val col = dialect.expr.collate(charset, collation)
        s"ALTER TABLE ${dialect.quote(table)} ALTER COLUMN ${dialect.quoteIdentifier(columnName)} $nullable$col"
    }

    override def updateColumnComment(table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): String = {
        s"COMMENT ON COLUMN ${dialect.quote(table)}.${dialect.quoteIdentifier(columnName)} IS ${comment.map{dialect.literal}.getOrElse("null")}"
    }
}



class PostgresCommands(dialect: BaseDialect) extends BaseCommands(dialect) {
    override def createTable(statement:Statement, table:TableDefinition) : Unit = {
        super.createTable(statement, table)

        // Attach any comments
        table.columns.foreach { col =>
            updateColumnComment(statement, table.identifier, col.name, col.typeName, col.nullable, comment=col.description)
        }
    }

    override def getJdbcSchema(statement:Statement, table:TableIdentifier) : Seq[JdbcField] = {
        // Get basic information
        val fields = super.getJdbcSchema(statement, table)

        // Query extended information
        val sql =
            s"""
               |SELECT
               |    column_name,
               |    collation_name
               |FROM information_schema.columns
               |WHERE lower(table_catalog) = current_catalog
               |  AND lower(table_schema) = ${table.space.headOption.map(s => dialect.literal(s.toLowerCase(Locale.ROOT))).getOrElse("lower(current_schema())")}
               |  AND lower(table_name) = ${dialect.literal(table.table.toLowerCase(Locale.ROOT))}
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
               |    cols.column_name, (
               |        SELECT
               |            pg_catalog.col_description(c.oid, cols.ordinal_position::int)
               |        FROM
               |            pg_catalog.pg_class c
               |        WHERE
               |            c.oid = (SELECT ('"' || cols.table_name || '"')::regclass::oid)
               |            AND c.relname = cols.table_name
               |    ) AS column_comment
               |FROM
               |    information_schema.columns cols
               |WHERE
               |    cols.table_catalog = current_catalog
               |    AND cols.table_schema = ${table.database.map(dialect.literal).getOrElse("current_schema()")}
               |    AND cols.table_name = ${dialect.literal(table.table)}
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

    override def addColumn(statement:Statement, table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): Unit = {
        super.addColumn(statement, table, columnName, dataType, isNullable, charset, collation, comment)
        comment.foreach { c =>
            val sql = dialect.statement.updateColumnComment(table, columnName, dataType, isNullable, charset, collation, comment)
            statement.executeUpdate(sql)
        }
    }

    override def updateColumnComment(statement:Statement, table: TableIdentifier, columnName:String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None) : Unit = {
        val sql = dialect.statement.updateColumnComment(table, columnName, dataType, isNullable, charset, collation, comment)
        statement.executeUpdate(sql)
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
        }
    }
}
