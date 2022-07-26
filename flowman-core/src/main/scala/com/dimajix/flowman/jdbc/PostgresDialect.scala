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
}



class PostgresCommands(dialect: BaseDialect) extends BaseCommands(dialect) {
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
        val rs = statement.executeQuery(sql)
        val colInfo = mutable.Map[String,String]()
        try {
            while (rs.next()) {
                val name = rs.getString(1)
                val collation = rs.getString(2)
                colInfo.put(name, collation)
            }
        }
        finally {
            rs.close()
        }

        // Merge base info and extra info
        fields.map { f =>
            colInfo.get(f.name)
                .map(c => f.copy(collation=Option(c)))
                .getOrElse(f)
        }
    }
}
