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

import java.nio.file.Path

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{types => st}

import com.dimajix.flowman.catalog
import com.dimajix.flowman.catalog.TableChange
import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableType
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.CharType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.VarcharType
import com.dimajix.spark.testing.LocalTempDir


class JdbcUtilsTest extends AnyFlatSpec with Matchers with LocalTempDir {
    var db:Path = _
    var url:String = _
    val driver = "org.apache.derby.jdbc.EmbeddedDriver"

    override def beforeAll() : Unit = {
        super.beforeAll()
        db = tempDir.toPath.resolve("mydb")
        url = "jdbc:derby:" + db + ";create=true"
    }

    "JdbcUtils.getTableSchema()" should "work" in {
        val options = new JDBCOptions(url, "table_001", Map(JDBCOptions.JDBC_DRIVER_CLASS -> driver))
        val conn = JdbcUtils.createConnection(options)
        val table = TableDefinition(
            TableIdentifier("table_001"),
            TableType.TABLE,
            Seq(
                Field("str_field", StringType),
                Field("int_field", IntegerType)
            ),
            None,
            None
        )
        JdbcUtils.createTable(conn, table, options)

        JdbcUtils.getTableSchema(conn, table.identifier, options) should be (
            StructType(Seq(
                Field("str_field", StringType),
                Field("int_field", IntegerType)
            ))
        )

        JdbcUtils.dropTable(conn, table.identifier, options)
        conn.close()
    }

    "JdbcUtils.alterTable()" should "work" in {
        val options = new JDBCOptions(url, "table_002", Map(JDBCOptions.JDBC_DRIVER_CLASS -> driver))
        val conn = JdbcUtils.createConnection(options)
        val table = catalog.TableDefinition(
            TableIdentifier("table_001"),
            TableType.TABLE,
            Seq(
                Field("str_field", VarcharType(20)),
                Field("int_field", IntegerType, nullable=false)
            ),
            None,
            None
        )
        JdbcUtils.createTable(conn, table, options)

        val curSchema = JdbcUtils.getTableSchema(conn, table.identifier, options)
        val newSchema = StructType(Seq(
            Field("str_field", VarcharType(30)),
            Field("int_field", IntegerType),
            Field("BOOL_FIELD", BooleanType)
        ))

        val curTable = TableDefinition(TableIdentifier(""), TableType.TABLE, curSchema.fields)
        val newTable = TableDefinition(TableIdentifier(""), TableType.TABLE, newSchema.fields)
        val migrations = TableChange.migrate(curTable, newTable, MigrationPolicy.STRICT)
        JdbcUtils.alterTable(conn, table.identifier, migrations, options)
        JdbcUtils.getTableSchema(conn, table.identifier, options) should be (newSchema)

        JdbcUtils.dropTable(conn, table.identifier, options)
        conn.close()
    }

    "JdbcUtils.createSchema()" should "work" in {
        val dataSchema = st.StructType(Seq(
            st.StructField("str_field", st.StringType, nullable=true),
            st.StructField("charn_field", st.StringType, nullable=false),
            st.StructField("vcharn_field", st.StringType, nullable=false),
            st.StructField("double_field", st.DoubleType, nullable=false),
            st.StructField("src_only_field", st.IntegerType, nullable=false)
        ))
        val tableSchema = StructType(Seq(
            Field("str_field", StringType, nullable=false),
            Field("charn_field", VarcharType(10), nullable=true),
            Field("vcharn_field", CharType(20), nullable=false),
            Field("double_field", FloatType, nullable=false),
            Field("tgt_only_field", IntegerType, nullable=false)
        ))

        val result = JdbcUtils.createSchema(dataSchema, tableSchema)
        result should be (StructType(Seq(
            Field("str_field", StringType, nullable=true),
            Field("charn_field", VarcharType(10), nullable=false),
            Field("vcharn_field", CharType(20), nullable=false),
            Field("double_field", DoubleType, nullable=false),
            Field("tgt_only_field", IntegerType, nullable=false)
        )))
    }
}
