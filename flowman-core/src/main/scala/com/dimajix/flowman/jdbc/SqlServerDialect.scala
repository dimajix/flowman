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

import java.util.Locale

import org.apache.spark.sql.jdbc.JdbcType

import com.dimajix.flowman.catalog.TableIdentifier
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

    override def statement : SqlStatements = Statements
}


class MsSqlServerStatements(dialect: BaseDialect) extends BaseStatements(dialect)  {
    override def firstRow(table: TableIdentifier, condition:String) : String = {
        if (condition.isEmpty)
            s"SELECT TOP 1 * FROM ${dialect.quote(table)}"
        else
            s"SELECT TOP 1 * FROM ${dialect.quote(table)} WHERE $condition"
    }

    // see https://docs.microsoft.com/en-us/sql/relational-databases/tables/add-columns-to-a-table-database-engine?view=sql-server-ver15
    override def addColumn(table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean): String = {
        val nullable = if (isNullable) "NULL" else "NOT NULL"
        s"ALTER TABLE ${dialect.quote(table)} ADD ${dialect.quoteIdentifier(columnName)} $dataType $nullable"
    }

    // See https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-rename-transact-sql?view=sql-server-ver15
    override def renameColumn(table: TableIdentifier, columnName: String, newName: String): String = {
        s"EXEC sp_rename '${dialect.quote(table)}.${dialect.quoteIdentifier(columnName)}', ${dialect.quoteIdentifier(newName)}, 'COLUMN'"
    }

    // see https://docs.microsoft.com/en-us/sql/t-sql/statements/alter-table-transact-sql?view=sql-server-ver15
    // require to have column data type to change the column nullability
    // ALTER TABLE tbl_name ALTER COLUMN col_name datatype [NULL | NOT NULL]
    // column_definition:
    //    data_type [NOT NULL | NULL]
    // We don't have column data type here, so we throw Exception for now
    override def updateColumnNullability(table: TableIdentifier, columnName: String, dataType:String, isNullable: Boolean): String = {
        val nullable = if (isNullable) "NULL" else "NOT NULL"
        s"ALTER TABLE ${dialect.quote(table)} ALTER COLUMN ${dialect.quoteIdentifier(columnName)} $dataType $nullable"
    }

    override def dropIndex(table: TableIdentifier, indexName: String): String = {
        s"DROP INDEX ${dialect.quote(table)}.${dialect.quoteIdentifier(indexName)}"
    }
}
