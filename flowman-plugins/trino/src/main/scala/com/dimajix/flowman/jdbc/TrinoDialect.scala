/*
 * Copyright (C) 2023 The Flowman Authors
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
import com.dimajix.flowman.types.BinaryType
import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.ByteType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.ShortType
import com.dimajix.flowman.types.StringType


class TrinoDialect extends BaseDialect {
    private object Statements extends TrinoStatements(this)
    private object Expressions extends TrinoExpressions(this)
    private object Commands extends TrinoCommands(this)

    override def canHandle(url : String): Boolean = url.startsWith("jdbc:trino")

    override def getJdbcType(dt: FieldType): JdbcType = dt match {
        case ByteType => JdbcType("TINYINT", java.sql.Types.TINYINT)
        case ShortType => JdbcType("SMALLINT", java.sql.Types.SMALLINT)
        case DoubleType => JdbcType("DOUBLE", java.sql.Types.DOUBLE)
        case BooleanType => JdbcType("BOOLEAN", java.sql.Types.BOOLEAN)
        case StringType => JdbcType("VARCHAR", java.sql.Types.CLOB)
        case BinaryType => JdbcType("VARBINARY", java.sql.Types.BLOB)
        case _ => super.getJdbcType(dt)
    }

    override def getFieldType(sqlType: Int, typeName:String, precision: Int, scale: Int, signed: Boolean): FieldType = {
        sqlType match {
            case Types.REAL => FloatType
            case Types.VARCHAR if precision <= 0 || precision >= 2147483647 => StringType
            case _ => super.getFieldType(sqlType, typeName, precision, scale, signed)
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
object TrinoDialect extends TrinoDialect


class TrinoExpressions(dialect: BaseDialect) extends BaseExpressions(dialect) {
    override def collate(charset:Option[String], collation:Option[String]) : String = {
        ""
    }

    override def comment(comment:Option[String]) : String = {
        comment.map(c => s" COMMENT ${dialect.literal(c)}").getOrElse("")
    }
}

class TrinoStatements(dialect: BaseDialect) extends BaseStatements(dialect)  {
    override def getViewDefinition(table: TableIdentifier): String = {
        s"""
           |SELECT
           |    view_definition
           |FROM information_schema.views
           |WHERE lower(table_catalog) = lower(current_catalog)
           |    AND lower(table_schema) = ${table.space.headOption.map(s => dialect.literal(s.toLowerCase(Locale.ROOT))).getOrElse("")}
           |    AND lower(table_name) = ${dialect.literal(table.table.toLowerCase(Locale.ROOT))}
           |""".stripMargin
    }
}


class TrinoCommands(dialect: BaseDialect) extends BaseCommands(dialect) {
    override def getJdbcSchema(statement: Statement, table: TableIdentifier): Seq[JdbcField] = {
        // Get basic information
        val fields = super.getJdbcSchema(statement, table)

        // Query extended information
        val sql = s"DESCRIBE ${dialect.quote(table)}"
        val rs = statement.executeQuery(sql)
        val colInfo = mutable.Map[String, Option[String]]()
        try {
            while (rs.next()) {
                val name = rs.getString(1)
                val comment = Option(rs.getString(4)).filter(_.nonEmpty)
                colInfo.put(name, comment)
            }
        }
        finally {
            rs.close()
        }

        // Merge base info and extra info
        fields.map { f =>
            colInfo.get(f.name)
                .map(c => f.copy(description=c))
                .getOrElse(f)
        }
    }
}
