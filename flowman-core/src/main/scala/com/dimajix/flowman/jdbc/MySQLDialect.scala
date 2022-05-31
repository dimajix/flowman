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

import java.sql.Types
import java.util.Locale

import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.BooleanType


object MySQLDialect extends BaseDialect {
    private object Statements extends MySQLStatements(this)

    override def canHandle(url : String): Boolean = url.startsWith("jdbc:mysql") || url.startsWith("jdbc:mariadb")

    override def quoteIdentifier(colName: String): String = {
        s"`$colName`"
    }

    override def getFieldType(sqlType: Int, typeName:String, precision: Int, scale: Int, signed: Boolean): FieldType = {
        if (sqlType == Types.VARBINARY && typeName.equals("BIT") && precision != 1) {
            LongType
        } else if (sqlType == Types.BIT && typeName.equals("TINYINT")) {
            BooleanType
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
}


class MySQLStatements(dialect: BaseDialect) extends BaseStatements(dialect)  {
    override def getViewDefinition(table: TableIdentifier): String = {
        s"""
           |SELECT
           |    VIEW_DEFINITION
           |FROM information_schema.VIEWS
           |WHERE lower(TABLE_SCHEMA) = ${table.space.headOption.map(s => dialect.literal(s.toUpperCase(Locale.ROOT))).getOrElse("lower(database())")}
           |    AND lower(TABLE_NAME) = ${dialect.literal(table.table.toUpperCase(Locale.ROOT))}
           |""".stripMargin
    }

    override def firstRow(table: TableIdentifier, condition:String) : String = {
        if (condition.isEmpty)
            s"SELECT * FROM ${dialect.quote(table)} FETCH FIRST ROW ONLY"
        else
            s"SELECT * FROM ${dialect.quote(table)} WHERE $condition FETCH FIRST ROW ONLY"
    }

    // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
    override def updateColumnType(table: TableIdentifier, columnName: String, newDataType: String, isNullable: Boolean): String = {
        val nullable = if (isNullable) "NULL" else "NOT NULL"
        s"ALTER TABLE ${dialect.quote(table)} MODIFY COLUMN ${dialect.quoteIdentifier(columnName)} $newDataType $nullable"
    }

    // See Old Syntax: https://dev.mysql.com/doc/refman/5.6/en/alter-table.html
    // According to https://dev.mysql.com/worklog/task/?id=10761 old syntax works for
    // both versions of MySQL i.e. 5.x and 8.0
    override def renameColumn(table: TableIdentifier, columnName: String, newName: String): String = {
        s"ALTER TABLE ${dialect.quote(table)} RENAME COLUMN ${dialect.quoteIdentifier(columnName)} TO ${dialect.quoteIdentifier(newName)}"
    }

    // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
    override def updateColumnNullability(table: TableIdentifier, columnName: String, dataType:String, isNullable: Boolean): String = {
        val nullable = if (isNullable) "NULL" else "NOT NULL"
        s"ALTER TABLE ${dialect.quote(table)} MODIFY COLUMN ${dialect.quoteIdentifier(columnName)} $dataType $nullable"
    }

    override def dropIndex(table: TableIdentifier, indexName: String): String = {
        s"DROP INDEX ${dialect.quoteIdentifier(indexName)} ON ${dialect.quote(table)}"
    }
}
