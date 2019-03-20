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

import java.nio.file.Path

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalTempDir
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType


class DerbyJdbcTest extends FlatSpec with Matchers with LocalTempDir {
    var db:Path = _
    var url:String = _
    val driver = "org.apache.derby.jdbc.EmbeddedDriver"

    override def beforeAll() : Unit = {
        super.beforeAll()
        db = tempDir.toPath.resolve("mydb")
        url = "jdbc:derby:" + db + ";create=true"
    }

    "A Derby Table" should "be creatable" in {
        val options = new JDBCOptions(url, "table_001", Map())
        val conn = JdbcUtils.createConnection(options)
        val table = TableDefinition(
            TableIdentifier("table_001"),
            Seq(
                Field("str_field", StringType),
                Field("int_field", IntegerType)
            ),
            "",
            Seq()
        )
        JdbcUtils.tableExists(conn, table.identifier, options) should be (false)
        JdbcUtils.createTable(conn, table, options)
        JdbcUtils.tableExists(conn, table.identifier, options) should be (true)
        JdbcUtils.dropTable(conn, table.identifier, options)
        JdbcUtils.tableExists(conn, table.identifier, options) should be (false)
        conn.close()
    }
}
