/*
 * Copyright 2018-2023 Kaya Kupferschmidt
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

import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.StringType


class MySQLDialect extends BaseDialect {
    private object Statements extends MySQLStatements(this)
    private object Expressions extends MySQLExpressions(this)
    private object Commands extends MySQLCommands(this)

    override def canHandle(url : String): Boolean = url.startsWith("jdbc:mysql")

    override def quoteIdentifier(colName: String): String = {
        s"`$colName`"
    }

    override def getJdbcType(dt: FieldType): JdbcType = dt match {
        case FloatType => JdbcType("FLOAT", java.sql.Types.FLOAT)
        case _ => super.getJdbcType(dt)
    }

    override def getFieldType(sqlType: Int, typeName:String, precision: Int, scale: Int, signed: Boolean): FieldType = {
        if (sqlType == Types.VARBINARY && typeName.equals("BIT") && precision != 1) {
            LongType
        } else if (sqlType == Types.BIT && typeName.equals("TINYINT")) {
            BooleanType
        } else if (sqlType == Types.REAL && typeName.equals("FLOAT")) {
            FloatType
        } else {
            super.getFieldType(sqlType, typeName, precision, scale, signed)
        }
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
object MySQLDialect extends MySQLDialect


class MySQLExpressions(dialect: BaseDialect) extends BaseExpressions(dialect) {
    override def collate(charset:Option[String], collation:Option[String]) : String = {
        val cs = charset.map(c => s" CHARACTER SET $c").getOrElse("")
        val col = collation.map(c => s" COLLATE $c").getOrElse("")
        cs + col
    }

    override def comment(comment:Option[String]) : String = {
        comment.map(c => s" COMMENT ${dialect.literal(c)}").getOrElse("")
    }
}

class MySQLStatements(dialect: BaseDialect) extends BaseStatements(dialect)  {
    override def getViewDefinition(table: TableIdentifier): String = {
        s"""
           |SELECT
           |    VIEW_DEFINITION
           |FROM information_schema.VIEWS
           |WHERE lower(TABLE_SCHEMA) = ${table.space.headOption.map(s => dialect.literal(s.toLowerCase(Locale.ROOT))).getOrElse("lower(database())")}
           |    AND lower(TABLE_NAME) = ${dialect.literal(table.table.toLowerCase(Locale.ROOT))}
           |""".stripMargin
    }

    override def firstRow(table: TableIdentifier, condition:String) : String = {
        if (condition.isEmpty)
            s"SELECT * FROM ${dialect.quote(table)} FETCH FIRST ROW ONLY"
        else
            s"SELECT * FROM ${dialect.quote(table)} WHERE $condition FETCH FIRST ROW ONLY"
    }

    override def addColumn(table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): String = {
        val nullable = if (isNullable) "NULL" else "NOT NULL"
        val desc = dialect.expr.comment(comment)
        val col = dialect.expr.collate(charset, collation)
        s"ALTER TABLE ${dialect.quote(table)} ADD COLUMN ${dialect.quoteIdentifier(columnName)} $dataType$col $nullable$desc"
    }

    // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
    override def updateColumnType(table: TableIdentifier, columnName: String, newDataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): String = {
        val nullable = if (isNullable) "NULL" else "NOT NULL"
        val desc = dialect.expr.comment(comment)
        val col = dialect.expr.collate(charset, collation)
        s"ALTER TABLE ${dialect.quote(table)} MODIFY COLUMN ${dialect.quoteIdentifier(columnName)} $newDataType$col $nullable$desc"
    }

    // See Old Syntax: https://dev.mysql.com/doc/refman/5.6/en/alter-table.html
    // According to https://dev.mysql.com/worklog/task/?id=10761 old syntax works for
    // both versions of MySQL i.e. 5.x and 8.0
    override def renameColumn(table: TableIdentifier, columnName: String, newName: String): String = {
        s"ALTER TABLE ${dialect.quote(table)} RENAME COLUMN ${dialect.quoteIdentifier(columnName)} TO ${dialect.quoteIdentifier(newName)}"
    }

    // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
    override def updateColumnNullability(table: TableIdentifier, columnName: String, dataType:String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): String = {
        val nullable = if (isNullable) "NULL" else "NOT NULL"
        val desc = dialect.expr.comment(comment)
        val col = dialect.expr.collate(charset, collation)
        s"ALTER TABLE ${dialect.quote(table)} MODIFY COLUMN ${dialect.quoteIdentifier(columnName)} $dataType$col $nullable$desc"
    }
    override def updateColumnComment(table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): String = {
        val nullable = if (isNullable) "NULL" else "NOT NULL"
        val desc = dialect.expr.comment(comment)
        val col = dialect.expr.collate(charset, collation)
        s"ALTER TABLE ${dialect.quote(table)} MODIFY COLUMN ${dialect.quoteIdentifier(columnName)} $dataType$col $nullable$desc"
    }

    override def dropPrimaryKey(table: TableIdentifier): String = {
        s"ALTER TABLE ${dialect.quote(table)} DROP PRIMARY KEY"
    }

    override def dropIndex(table: TableIdentifier, indexName: String): String = {
        s"DROP INDEX ${dialect.quoteIdentifier(indexName)} ON ${dialect.quote(table)}"
    }
}


class MySQLCommands(dialect: BaseDialect) extends BaseCommands(dialect) {
    override def getJdbcSchema(statement: Statement, table: TableIdentifier): Seq[JdbcField] = {
        // Get basic information
        val fields = super.getJdbcSchema(statement, table)

        // Query extended information
        val sql =
            s"""
               |SELECT
               |    COLUMN_NAME,
               |    CHARACTER_SET_NAME,
               |    COLLATION_NAME,
               |    COLUMN_COMMENT
               |FROM information_schema.COLUMNS
               |WHERE lower(TABLE_SCHEMA) = ${table.space.headOption.map(s => dialect.literal(s.toLowerCase(Locale.ROOT))).getOrElse("lower(database())")}
               |    AND lower(TABLE_NAME) = ${dialect.literal(table.table.toLowerCase(Locale.ROOT))}
               |""".stripMargin
        val rs = statement.executeQuery(sql)
        val colInfo = mutable.Map[String, (Option[String], Option[String], Option[String])]()
        try {
            while (rs.next()) {
                val name = rs.getString(1)
                val charset = Option(rs.getString(2))
                val collation = Option(rs.getString(3))
                val comment = Option(rs.getString(4)).filter(_.nonEmpty)
                colInfo.put(name, (charset, collation, comment))
            }
        }
        finally {
            rs.close()
        }

        // Merge base info and extra info
        fields.map { f =>
            colInfo.get(f.name)
                .map(c => f.copy(charset=c._1, collation=c._2, description=c._3))
                .getOrElse(f)
        }
    }

    override def updateColumnComment(statement:Statement, table: TableIdentifier, columnName:String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None) : Unit = {
        val sql = dialect.statement.updateColumnComment(table, columnName, dataType, isNullable, charset, collation, comment)
        JdbcUtils.executeUpdate(statement, sql)
    }
}
