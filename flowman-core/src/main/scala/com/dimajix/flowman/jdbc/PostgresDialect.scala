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
     * Quotes the identifier. This is used to put quotes around the identifier in case the column
     * name is a reserved keyword, or in case it contains characters that require quotes (e.g. space).
     */
    override def quoteIdentifier(colName: String): String = {
        s"""`$colName`"""
    }

    override def statement : SqlStatements = Statements
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

    override def updateColumnType(table: TableIdentifier, columnName: String, newDataType: String, isNullable: Boolean): String =
        s"ALTER TABLE ${dialect.quote(table)} ALTER COLUMN ${dialect.quoteIdentifier(columnName)} TYPE $newDataType"

    override def updateColumnNullability(table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean): String = {
        val nullable = if (isNullable) "DROP NOT NULL" else "SET NOT NULL"
        s"ALTER TABLE ${dialect.quote(table)} ALTER COLUMN ${dialect.quoteIdentifier(columnName)} $nullable"
    }
}
