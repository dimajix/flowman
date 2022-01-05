/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.jdbc.JdbcType

import com.dimajix.flowman.catalog.TableChange
import com.dimajix.flowman.catalog.TableChange.AddColumn
import com.dimajix.flowman.catalog.TableChange.DropColumn
import com.dimajix.flowman.catalog.TableChange.UpdateColumnComment
import com.dimajix.flowman.catalog.TableChange.UpdateColumnNullability
import com.dimajix.flowman.catalog.TableChange.UpdateColumnType
import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.ByteType
import com.dimajix.flowman.types.DecimalType
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.ShortType
import com.dimajix.flowman.types.StringType


object DerbyDialect extends BaseDialect {
    private object Statements extends DerbyStatements(this)

    override def canHandle(url: String): Boolean = url.startsWith("jdbc:derby")

    /**
     * Quotes a table name including the optional database prefix
     * @param table
     * @return
     */
    override def quote(table:TableIdentifier) : String = {
        if (table.database.isDefined)
            table.database.get + "." + table.table
        else
            table.table
    }

    override def getJdbcType(dt: FieldType): JdbcType = dt match {
        case StringType => JdbcType("CLOB", java.sql.Types.CLOB)
        case ByteType => JdbcType("SMALLINT", java.sql.Types.SMALLINT)
        case ShortType => JdbcType("SMALLINT", java.sql.Types.SMALLINT)
        case BooleanType => JdbcType("BOOLEAN", java.sql.Types.BOOLEAN)
        // 31 is the maximum precision and 5 is the default scale for a Derby DECIMAL
        case t: DecimalType if t.precision > 31 =>
            JdbcType("DECIMAL(31,5)", java.sql.Types.DECIMAL)
        case _ => super.getJdbcType(dt)
    }

    override def getFieldType(sqlType: Int, typeName:String, precision: Int, scale: Int, signed: Boolean): FieldType = {
        sqlType match {
            case java.sql.Types.REAL => FloatType
            case _ => super.getFieldType(sqlType, typeName, precision, scale, signed)
        }
    }

    /**
     * Returns true if the given table supports a specific table change
     * @param change
     * @return
     */
    override def supportsChange(table:TableIdentifier, change:TableChange) : Boolean = {
        change match {
            case _:UpdateColumnType => false
            case x:TableChange => super.supportsChange(table, change)
        }
    }

    override def statement : SqlStatements = Statements
}


class DerbyStatements(dialect: BaseDialect) extends BaseStatements(dialect)  {
    override def firstRow(table: TableIdentifier, condition:String) : String = {
        if (condition.isEmpty)
            s"SELECT * FROM ${dialect.quote(table)} FETCH FIRST ROW ONLY"
        else
            s"SELECT * FROM ${dialect.quote(table)} WHERE $condition FETCH FIRST ROW ONLY"
    }

    override def updateColumnType(table: TableIdentifier, columnName: String, newDataType: String): String =
        s"ALTER TABLE ${dialect.quote(table)} ALTER COLUMN ${dialect.quoteIdentifier(columnName)} SET DATA TYPE $newDataType"
}
