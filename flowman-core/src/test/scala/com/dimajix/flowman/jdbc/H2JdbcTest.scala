/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

import java.nio.file.Path
import java.util.Properties

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructField
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.catalog
import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableIndex
import com.dimajix.flowman.catalog.TableType
import com.dimajix.flowman.execution.DeleteClause
import com.dimajix.flowman.execution.InsertClause
import com.dimajix.flowman.execution.UpdateClause
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.VarcharType
import com.dimajix.spark.sql.DataFrameBuilder
import com.dimajix.spark.testing.LocalSparkSession


class H2JdbcTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    var db:Path = _
    var url:String = _
    val driver = "org.h2.Driver"

    override def beforeAll() : Unit = {
        super.beforeAll()
        db = tempDir.toPath.resolve("mydb")
        url = "jdbc:h2:" + db
    }

    "A H2 Table" should "be creatable" in {
        val options = new JDBCOptions(url, "table_001", Map(JDBCOptions.JDBC_DRIVER_CLASS -> driver))
        val conn = JdbcUtils.createConnection(options)
        val table = TableDefinition(
            TableIdentifier("table_001"),
            TableType.TABLE,
            columns = Seq(
                Field("Id", IntegerType, nullable=false),
                Field("str_field", VarcharType(32)),
                Field("int_field", IntegerType)
            ),
            primaryKey = Seq("iD"),
            indexes = Seq(
                TableIndex("table_001_idx1", Seq("str_field", "int_field"))
            )
        )

        //==== CREATE ================================================================================================
        JdbcUtils.tableExists(conn, table.identifier, options) should be (false)
        JdbcUtils.createTable(conn, table, options)
        JdbcUtils.tableExists(conn, table.identifier, options) should be (true)

        JdbcUtils.getTable(conn, table.identifier, options).normalize() should be (table.normalize())

        //==== DROP INDEX ============================================================================================
        val table2 = table.copy(indexes = Seq.empty)
        JdbcUtils.dropIndex(conn, table.identifier, "table_001_idx1", options)
        JdbcUtils.getTable(conn, table.identifier, options).normalize() should be (table2.normalize())

        //==== CREATE INDEX ============================================================================================
        val table3 = table2.copy(indexes = Seq(TableIndex("table_001_idx1", Seq("str_field", "Id"))))
        JdbcUtils.createIndex(conn, table3.identifier, table3.indexes.head, options)
        JdbcUtils.getTable(conn, table3.identifier, options).normalize() should be (table3.normalize())

        //==== DROP ==================================================================================================
        JdbcUtils.dropTable(conn, table.identifier, options)
        JdbcUtils.tableExists(conn, table.identifier, options) should be (false)
        conn.close()
    }

    "JdbcUtils.mergeTable()" should "work with complex clauses" in {
        val options = new JDBCOptions(url, "table_002", Map(JDBCOptions.JDBC_DRIVER_CLASS -> driver))
        val conn = JdbcUtils.createConnection(options)
        val table = catalog.TableDefinition(
            TableIdentifier("table_001"),
            TableType.TABLE,
            Seq(
                Field("id", IntegerType),
                Field("Name", StringType),
                Field("Sex", StringType),
                Field("State", StringType)
            ),
            None,
            Seq()
        )

        // ===== Create Table =========================================================================================
        JdbcUtils.tableExists(conn, table.identifier, options) should be (false)
        JdbcUtils.createTable(conn, table, options)
        JdbcUtils.tableExists(conn, table.identifier, options) should be (true)

        // ===== Write Table ==========================================================================================
        val tableSchema = org.apache.spark.sql.types.StructType(Seq(
            StructField("id", org.apache.spark.sql.types.IntegerType),
            StructField("name", org.apache.spark.sql.types.StringType),
            StructField("sex", org.apache.spark.sql.types.StringType),
            StructField("state", org.apache.spark.sql.types.StringType)
        ))
        val df0 = DataFrameBuilder.ofRows(
            spark,
            Seq(
                Row(10, "Alice", "male", "mutable"),
                Row(20, "Bob", "male", "immutable"),
                Row(30, "Chris", "male", "mutable"),
                Row(40, "Eve", "female", "immutable")
            ),
            tableSchema
        )
        df0.write.mode("APPEND").jdbc(url, "table_001", new Properties())

        // ===== Read Table ===========================================================================================
        val df1 = spark.read.jdbc(url, "table_001", new Properties())
        df1.sort(col("id")).collect() should be (Seq(
            Row(10, "Alice", "male", "mutable"),
            Row(20, "Bob", "male", "immutable"),
            Row(30, "Chris", "male", "mutable"),
            Row(40, "Eve", "female", "immutable")
        ))

        // ===== Merge Table ==========================================================================================
        val updateSchema = org.apache.spark.sql.types.StructType(Seq(
            StructField("id", org.apache.spark.sql.types.IntegerType),
            StructField("name", org.apache.spark.sql.types.StringType),
            StructField("sex", org.apache.spark.sql.types.StringType),
            StructField("op", org.apache.spark.sql.types.StringType)
        ))
        val df2 = DataFrameBuilder.ofRows(
            spark,
            Seq(
                Row(10, "Alice", "female", "UPDATE"),
                Row(20, null, null, "UPDATE"),
                Row(30, null, null, "DELETE"),
                Row(40, null, null, "DELETE"),
                Row(50, "Debora", "female", "INSERT")
            ),
            updateSchema
        )
        val condition = expr("source.id = target.id")
        val clauses = Seq(
            InsertClause(
                condition = Some(expr("source.op = 'INSERT'")),
                columns = Map("id" -> expr("source.id"), "name" -> expr("source.name"), "sex" -> expr("source.sex"), "state" -> lit("mutable"))
            ),
            DeleteClause(
                condition = Some(expr("source.op = 'DELETE' AND upper(target.state) <> 'IMMUTABLE'"))
            ),
            UpdateClause(
                condition = Some(expr("source.op = 'UPDATE' AND upper(target.state) <> 'IMMUTABLE'")),
                columns = Map("name" -> expr("source.name"), "sex" -> expr("source.sex"))
            )
        )
        JdbcUtils.mergeTable(table.identifier, "target", Some(tableSchema), df2, "source", condition, clauses, options)

        // ===== Read Table ===========================================================================================
        val df3 = spark.read.jdbc(url, "table_001", new Properties())
        df3.sort(col("id")).collect() should be (Seq(
            Row(10, "Alice", "female", "mutable"),
            Row(20, "Bob", "male", "immutable"),
            Row(40, "Eve", "female", "immutable"),
            Row(50, "Debora", "female", "mutable")
        ))

        // ===== Drop Table ===========================================================================================
        JdbcUtils.dropTable(conn, table.identifier, options)
        JdbcUtils.tableExists(conn, table.identifier, options) should be (false)
        conn.close()
    }

    it should "work with trivial clauses" in {
        val options = new JDBCOptions(url, "table_002", Map(JDBCOptions.JDBC_DRIVER_CLASS -> driver))
        val conn = JdbcUtils.createConnection(options)
        val table = catalog.TableDefinition(
            TableIdentifier("table_001"),
            TableType.TABLE,
            Seq(
                Field("id", IntegerType),
                Field("Name", StringType),
                Field("Sex", StringType)
            ),
            None,
            Seq("id")
        )

        // ===== Create Table =========================================================================================
        JdbcUtils.tableExists(conn, table.identifier, options) should be (false)
        JdbcUtils.createTable(conn, table, options)
        JdbcUtils.tableExists(conn, table.identifier, options) should be (true)

        // ===== Write Table ==========================================================================================
        val tableSchema = org.apache.spark.sql.types.StructType(Seq(
            StructField("id", org.apache.spark.sql.types.IntegerType),
            StructField("name", org.apache.spark.sql.types.StringType),
            StructField("sex", org.apache.spark.sql.types.StringType)
        ))
        val df0 = DataFrameBuilder.ofRows(
            spark,
            Seq(
                Row(10, "Alice", "male"),
                Row(20, "Bob", "male")
            ),
            tableSchema
        )
        df0.write.mode("APPEND").jdbc(url, "table_001", new Properties())

        // ===== Read Table ===========================================================================================
        val df1 = spark.read.jdbc(url, "table_001", new Properties())
        df1.sort(col("id")).collect() should be (Seq(
            Row(10, "Alice", "male"),
            Row(20, "Bob", "male")
        ))

        // ===== Merge Table ==========================================================================================
        val updateSchema = org.apache.spark.sql.types.StructType(Seq(
            StructField("id", org.apache.spark.sql.types.IntegerType),
            StructField("name", org.apache.spark.sql.types.StringType),
            StructField("sex", org.apache.spark.sql.types.StringType)
        ))
        val df2 = DataFrameBuilder.ofRows(
            spark,
            Seq(
                Row(10, "Alice", "female"),
                Row(50, "Debora", "female")
            ),
            updateSchema
        )
        val condition = expr("source.id = target.id")
        val clauses = Seq(
            InsertClause(),
            UpdateClause()
        )
        JdbcUtils.mergeTable(table.identifier, "target", Some(tableSchema), df2, "source", condition, clauses, options)

        // ===== Read Table ===========================================================================================
        val df3 = spark.read.jdbc(url, "table_001", new Properties())
        df3.sort(col("id")).collect() should be (Seq(
            Row(10, "Alice", "female"),
            Row(20, "Bob", "male"),
            Row(50, "Debora", "female")
        ))

        // ===== Drop Table ===========================================================================================
        JdbcUtils.dropTable(conn, table.identifier, options)
        JdbcUtils.tableExists(conn, table.identifier, options) should be (false)
        conn.close()
    }
}
