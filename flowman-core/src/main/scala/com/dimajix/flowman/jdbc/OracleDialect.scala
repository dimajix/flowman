/*
 * Copyright 2022-2023 Kaya Kupferschmidt
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

import java.sql.Date
import java.sql.ResultSet
import java.sql.Statement
import java.sql.Timestamp
import java.sql.Types
import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.jdbc.JdbcType

import com.dimajix.flowman.catalog.PrimaryKey
import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableIndex
import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.ByteType
import com.dimajix.flowman.types.CharType
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


class OracleDialect extends BaseDialect {
    private object Statements extends OracleStatements(this)
    private object Commands extends OracleCommands(this)

    private[jdbc] val BINARY_FLOAT = 100
    private[jdbc] val BINARY_DOUBLE = 101
    private[jdbc] val TIMESTAMPTZ = -101

    override def canHandle(url : String): Boolean = url.startsWith("jdbc:oracle")

    override def getJdbcType(dt: FieldType): JdbcType = dt match {
        case BooleanType => JdbcType("NUMBER(1)", java.sql.Types.BOOLEAN)
        case IntegerType => JdbcType("NUMBER(10)", java.sql.Types.INTEGER)
        case LongType => JdbcType("NUMBER(19)", java.sql.Types.BIGINT)
        case FloatType => JdbcType("BINARY_FLOAT", java.sql.Types.FLOAT)
        case DoubleType => JdbcType("BINARY_DOUBLE", java.sql.Types.DOUBLE)
        case ByteType => JdbcType("NUMBER(3)", java.sql.Types.SMALLINT)
        case ShortType => JdbcType("NUMBER(5)", java.sql.Types.SMALLINT)
        case StringType => JdbcType("NVARCHAR2(255)", java.sql.Types.NVARCHAR)
        case v : CharType => JdbcType(s"NCHAR(${v.length})", java.sql.Types.NCHAR)
        case v : VarcharType => JdbcType(s"NVARCHAR2(${v.length})", java.sql.Types.NVARCHAR)
        case _ => super.getJdbcType(dt)
    }

    override def getFieldType(sqlType: Int, typeName:String, precision: Int, scale: Int, signed: Boolean): FieldType = {
        sqlType match {
            case Types.NUMERIC =>
                (precision,scale) match {
                    // Handle NUMBER fields that have no precision/scale in special way
                    // because JDBC ResultSetMetaData converts this to 0 precision and -127 scale
                    // For more details, please see
                    // https://github.com/apache/spark/pull/8780#issuecomment-145598968
                    // and
                    // https://github.com/apache/spark/pull/8780#issuecomment-144541760
                    case (0,_) => DecimalType(DecimalType.MAX_PRECISION, 10)
                    // Handle FLOAT fields in a special way because JDBC ResultSetMetaData converts
                    // this to NUMERIC with -127 scale
                    // Not sure if there is a more robust way to identify the field as a float (or other
                    // numeric types that do not specify a scale.
                    case (_,-127) => DecimalType(DecimalType.MAX_PRECISION, 10)
                    case _ => super.getFieldType(sqlType, typeName, precision, scale, signed)
                }
            case java.sql.Types.TIMESTAMP if typeName == "DATE" => DateType
            case TIMESTAMPTZ => TimestampType // Value for Timestamp with Time Zone in Oracle
            case BINARY_FLOAT => FloatType // Value for OracleTypes.BINARY_FLOAT
            case BINARY_DOUBLE => DoubleType // Value for OracleTypes.BINARY_DOUBLE
            case _ => super.getFieldType(sqlType, typeName, precision, scale, signed)
        }
    }

    override def literal(value: Any): String = value match {
        // The JDBC drivers support date literals in SQL statements written in the
        // format: {d 'yyyy-mm-dd'} and timestamp literals in SQL statements written
        // in the format: {ts 'yyyy-mm-dd hh:mm:ss.f...'}. For details, see
        // 'Oracle Database JDBC Developer’s Guide and Reference, 11g Release 1 (11.1)'
        // Appendix A Reference Information.
        case stringValue: String => s"'${escape(stringValue)}'"
        case timestampValue: Timestamp => "{ts '" + timestampValue + "'}"
        case ts: UtcTimestamp => s"timestamp(${ts.toEpochSeconds()})"
        case dateValue: Date => "{d '" + dateValue + "'}"
        case arrayValue: Array[Any] => arrayValue.map(literal).mkString(", ")
        case _ => value.toString
    }

    override def statement : SqlStatements = Statements
    override def command: SqlCommands = Commands
}
object OracleDialect extends OracleDialect


class OracleStatements(dialect: BaseDialect) extends BaseStatements(dialect) {
    override def firstRow(table: TableIdentifier, condition:String) : String = {
        if (condition.isEmpty)
            s"SELECT * FROM (SELECT * FROM ${dialect.quote(table)}) WHERE ROWNUM <= 1"
        else
            s"SELECT * FROM (SELECT * FROM ${dialect.quote(table)} WHERE $condition) WHERE ROWNUM <= 1"
    }

    override def addColumn(table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): String = {
        val nullable = if (isNullable) "NULL" else "NOT NULL"
        val desc = dialect.expr.comment(comment)
        val col = dialect.expr.collate(charset, collation)
        s"ALTER TABLE ${dialect.quote(table)} ADD ${dialect.quoteIdentifier(columnName)} $dataType$col $nullable$desc"
    }

    override def updateColumnType(table: TableIdentifier, columnName: String, newDataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): String = {
        val col = dialect.expr.collate(charset, collation)
        s"ALTER TABLE ${dialect.quote(table)} MODIFY ${dialect.quoteIdentifier(columnName)} $newDataType$col"
    }

    // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
    override def updateColumnNullability(table: TableIdentifier, columnName: String, dataType:String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): String = {
        val nullable = if (isNullable) "NULL" else "NOT NULL"
        s"ALTER TABLE ${dialect.quote(table)} MODIFY ${dialect.quoteIdentifier(columnName)} $nullable"
    }

    override def dropPrimaryKey(table: TableIdentifier): String = {
        s"ALTER TABLE ${dialect.quote(table)} DROP PRIMARY KEY"
    }

    override def updateColumnComment(table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): String = {
        s"COMMENT ON COLUMN ${dialect.quote(table)}.${dialect.quoteIdentifier(columnName)} IS ${comment.map{dialect.literal}.getOrElse("''")}"
    }
}


class OracleCommands(dialect: BaseDialect) extends BaseCommands(dialect) {
    override def createTable(statement: Statement, table: TableDefinition): Unit = {
        super.createTable(statement, table)

        // Attach any comments
        table.columns.foreach { col =>
            updateColumnComment(statement, table.identifier, col.name, col.typeName, col.nullable, comment = col.description)
        }
    }

    override def getJdbcSchema(statement:Statement, table:TableIdentifier) : Seq[JdbcField] = {
        // Get basic information
        val fields = super.getJdbcSchema(statement, table)

        val comments = getColumnComments(statement, table)

        // Merge base info and extra info
        fields.map { f =>
            comments.get(f.name)
                .map(c => f.copy(description=Option(c)))
                .getOrElse(f)
        }
    }

    override def getPrimaryKey(statement:Statement, table:TableIdentifier) : Option[PrimaryKey] = {
        // Query extended information
        val sql =
            s"""
               |SELECT
               |    column_name
               |FROM SYS.all_cons_columns WHERE constraint_name = (
               |  SELECT constraint_name FROM SYS.all_constraints
               |  WHERE LOWER(table_name) = ${dialect.literal(table.table.toLowerCase(Locale.ROOT))} AND CONSTRAINT_TYPE = 'P'
               |)
               |""".stripMargin

        val idxcols = mutable.ListBuffer[String]()
        query(statement,sql) { rs =>
            val name = rs.getString(1)
            idxcols.append(name)
        }

        if (idxcols.nonEmpty)
            Some(PrimaryKey(idxcols))
        else
            None
    }

    override def getIndexes(statement: Statement, table: TableIdentifier): Seq[TableIndex] = {
        // Query extended information
        val sql =
            s"""
               |SELECT
               |    c.INDEX_NAME,
               |    c.COLUMN_NAME,
               |    i.UNIQUENESS
               |FROM SYS.ALL_IND_COLUMNS c
               |LEFT JOIN SYS.ALL_INDEXES i
               |    ON c.INDEX_NAME = i.INDEX_NAME
               |WHERE lower(c.table_name) = ${dialect.literal(table.table.toLowerCase(Locale.ROOT))}
               |""".stripMargin

        val idxcols = mutable.ListBuffer[(String,String,Boolean)]()
        query(statement,sql) { rs =>
            val name = rs.getString(1)
            val column = rs.getString(2)
            val unique = rs.getString(3) == "UNIQUE"
            idxcols.append((name,column,unique))
        }

        idxcols
            .groupBy(_._1).map { case(name,cols) =>
            TableIndex(name, cols.map(_._2), cols.foldLeft(false)(_ || _._3))
        }.toSeq
    }

    private def getColumnComments(statement:Statement, table:TableIdentifier) : Map[String,String] = {
        // Query extended information
        val sql =
            s"""
               |SELECT
               |    COLUMN_NAME,
               |    COMMENTS
               |FROM sys.user_col_comments
               |WHERE lower(TABLE_NAME) = ${dialect.literal(table.table.toLowerCase(Locale.ROOT))}
               |""".stripMargin
        queryKeyValue(statement, sql)
    }
    private def queryKeyValue(statement:Statement, sql:String) : Map[String,String] = {
        val values = mutable.Map[String,String]()
        query(statement,sql) { rs =>
            val name = rs.getString(1)
            val value = rs.getString(2)
            if (value != null)
                values.put(name, value)
        }

        values.toMap
    }

    override def addColumn(statement:Statement, table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): Unit = {
        super.addColumn(statement, table, columnName, dataType, isNullable, charset, collation, comment)
        comment.foreach { c =>
            val sql = dialect.statement.updateColumnComment(table, columnName, dataType, isNullable, charset, collation, comment)
            JdbcUtils.executeUpdate(statement, sql)
        }
    }

    override def updateColumnComment(statement:Statement, table: TableIdentifier, columnName:String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None) : Unit = {
        val sql = dialect.statement.updateColumnComment(table, columnName, dataType, isNullable, charset, collation, comment)
        JdbcUtils.executeUpdate(statement, sql)
    }
}
