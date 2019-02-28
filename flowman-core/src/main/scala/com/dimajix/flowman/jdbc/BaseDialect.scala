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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.jdbc.JdbcType

import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.types.BinaryType
import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.ByteType
import com.dimajix.flowman.types.DateType
import com.dimajix.flowman.types.DecimalType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.ShortType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.TimestampType
import com.dimajix.flowman.types.VarcharType
import com.dimajix.flowman.util.UtcTimestamp


abstract class BaseDialect extends SqlDialect {
    private object Statements extends BaseStatements(this) { }
    private object Expressions extends BaseExpressions(this) { }

    /**
      * Retrieve the jdbc / sql type for a given datatype.
      * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
      * @return The new JdbcType if there is an override for this DataType
      */
    override def getJdbcType(dt: FieldType): Option[JdbcType] = {
        dt match {
            case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
            case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
            case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
            case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
            case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
            case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
            case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
            case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
            case v : VarcharType => Option(JdbcType(s"VARCHAR(${v.length})", java.sql.Types.VARCHAR))
            case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
            case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
            case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
            case t: DecimalType => Option(JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
            case _ => None
        }
    }

    /**
      * Quotes the identifier. This is used to put quotes around the identifier in case the column
      * name is a reserved keyword, or in case it contains characters that require quotes (e.g. space).
      */
    override def quoteIdentifier(colName: String): String = {
        s""""$colName""""
    }

    override def quote(table:TableIdentifier) : String = {
        if (table.database.isDefined)
            quoteIdentifier(table.database.get) + "." + quoteIdentifier(table.table)
        else
            quoteIdentifier(table.table)
    }

    override def escape(value: String): String = {
        if (value == null) null
        else StringUtils.replace(value, "'", "''")
    }

    override def literal(value:Any) : String = {
        value match {
            case s:String => "'" + escape(s) + "'"
            case ts:UtcTimestamp => ts.toEpochSeconds().toString
            case v:Any =>  v.toString
        }
    }

    override def statement : SqlStatements = Statements

    override def expr : SqlExpressions = Expressions
}


class BaseStatements(dialect: SqlDialect) extends SqlStatements {
    override def create(table: TableDefinition): String = {
        val strSchema = table.fields.map { field =>
            val name = dialect.quoteIdentifier(field.name)
            val typ = dialect.getJdbcType(field.ftype)
                .getOrElse(throw new IllegalArgumentException(s"Can't get JDBC type for ${field.ftype}"))
                .databaseTypeDefinition
            val nullable = if (field.nullable) ""
            else "NOT NULL"
            s"$name $typ $nullable"
        }.mkString(",\n")
        val createTableOptions = ""

        s"CREATE TABLE ${dialect.quote(table.identifier)} ($strSchema) $createTableOptions"
    }

    override def tableExists(table: TableIdentifier) : String = {
        s"SELECT * FROM ${dialect.quote(table)} WHERE 1=0"
    }
}

class BaseExpressions(dialect: SqlDialect) extends SqlExpressions {
    override def in(column: String, values: Iterable[Any]): String = {
        column + " IN (" + values.map(dialect.literal).mkString(",") + ")"
    }

    override def eq(column: String, value: Any): String = {
        column + "=" + dialect.literal(value)
    }

    override def partition(partition: PartitionSpec): String = {
        val partitionValues = partition.values.map { case (k, v) => eq(k, v) }
        s"PARTITION(${partitionValues.mkString(",")})"
    }
}
