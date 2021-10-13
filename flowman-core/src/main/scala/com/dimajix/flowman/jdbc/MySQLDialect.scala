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

import java.sql.SQLFeatureNotSupportedException
import java.sql.Types

import org.apache.spark.sql.catalyst.TableIdentifier

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

    override def statement : SqlStatements = Statements
}


class MySQLStatements(dialect: BaseDialect) extends BaseStatements(dialect)  {
    override def firstRow(table: TableIdentifier, condition:String) : String = {
        if (condition.isEmpty)
            s"SELECT * FROM ${dialect.quote(table)} FETCH FIRST ROW ONLY"
        else
            s"SELECT * FROM ${dialect.quote(table)} WHERE $condition FETCH FIRST ROW ONLY"
    }

    // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
    override def updateColumnType(table: TableIdentifier, columnName: String, newDataType: String): String = {
        s"ALTER TABLE ${dialect.quote(table)} MODIFY COLUMN ${dialect.quoteIdentifier(columnName)} $newDataType"
    }

    // See Old Syntax: https://dev.mysql.com/doc/refman/5.6/en/alter-table.html
    // According to https://dev.mysql.com/worklog/task/?id=10761 old syntax works for
    // both versions of MySQL i.e. 5.x and 8.0
    // The old syntax requires us to have type definition. Since we do not have type
    // information, we throw the exception for old version.
    override def renameColumn(table: TableIdentifier, columnName: String, newName: String): String = {
        s"ALTER TABLE ${dialect.quote(table)} RENAME COLUMN ${dialect.quoteIdentifier(columnName)} TO ${dialect.quoteIdentifier(newName)}"
    }

    // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
    // require to have column data type to change the column nullability
    // ALTER TABLE tbl_name MODIFY [COLUMN] col_name column_definition
    // column_definition:
    //    data_type [NOT NULL | NULL]
    // e.g. ALTER TABLE t1 MODIFY b INT NOT NULL;
    // We don't have column data type here, so throw Exception for now
    override def updateColumnNullability(table: TableIdentifier, columnName: String, dataType:String, isNullable: Boolean): String = {
        throw new SQLFeatureNotSupportedException(s"UpdateColumnNullability is not supported")
    }
}
