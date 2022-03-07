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

import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableIndex
import com.dimajix.flowman.catalog.TableType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.VarcharType
import com.dimajix.spark.testing.LocalTempDir


class DerbyJdbcTest extends AnyFlatSpec with Matchers with LocalTempDir {
    var db:Path = _
    var url:String = _
    val driver = "org.apache.derby.jdbc.EmbeddedDriver"

    override def beforeAll() : Unit = {
        super.beforeAll()
        db = tempDir.toPath.resolve("mydb")
        url = "jdbc:derby:" + db + ";create=true"
    }

    "A Derby Table" should "be creatable" in {
        val options = new JDBCOptions(url, "table_001", Map(JDBCOptions.JDBC_DRIVER_CLASS -> driver))
        val conn = JdbcUtils.createConnection(options)
        val table1 = TableDefinition(
            TableIdentifier("table_001"),
            TableType.TABLE,
            Seq(
                Field("Id", IntegerType, nullable=false),
                Field("str_field", VarcharType(32)),
                Field("int_field", IntegerType)
            ),
            primaryKey = Seq("Id"),
            indexes = Seq(
                TableIndex("table_001_idx1", Seq("str_field", "int_field"))
            )
        )

        //==== CREATE ================================================================================================
        JdbcUtils.tableExists(conn, table1.identifier, options) should be (false)
        JdbcUtils.createTable(conn, table1, options)
        JdbcUtils.tableExists(conn, table1.identifier, options) should be (true)

        JdbcUtils.getTable(conn, table1.identifier, options) should be (table1)

        //==== DROP INDEX ============================================================================================
        val table2 = table1.copy(indexes = Seq.empty)
        JdbcUtils.dropIndex(conn, table1.identifier, "table_001_idx1", options)
        JdbcUtils.getTable(conn, table1.identifier, options) should be (table2)

        //==== CREATE INDEX ============================================================================================
        val table3 = table2.copy(indexes = Seq(TableIndex("table_001_idx1", Seq("str_field", "Id"))))
        JdbcUtils.createIndex(conn, table3.identifier, table3.indexes.head, options)
        JdbcUtils.getTable(conn, table3.identifier, options) should be (table3)

        //==== DROP ==================================================================================================
        JdbcUtils.dropTable(conn, table1.identifier, options)
        JdbcUtils.tableExists(conn, table1.identifier, options) should be (false)
        conn.close()
    }
}
