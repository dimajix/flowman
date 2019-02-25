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

package com.dimajix.flowman.spec.model

import java.sql.Driver
import java.sql.DriverManager
import java.sql.Statement
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module


class JdbcRelationTest extends FlatSpec with Matchers with LocalSparkSession {
    def withDatabase[T](driverClass:String, url:String)(fn:(Statement) => T) : T = {
        DriverRegistry.register(driverClass)
        val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
            case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == driverClass => d
            case d if d.getClass.getCanonicalName == driverClass => d
        }.getOrElse {
            throw new IllegalStateException(
                s"Did not find registered driver with class $driverClass")
        }
        val con = driver.connect(url, new Properties())
        try {
            val statement = con.createStatement()
            try {
                fn(statement)
            }
            finally {
                statement.close()
            }
        }
        finally {
            con.close()
        }
    }

    "The JdbcRelation" should "support create" in {
        val db = tempDir.toPath.resolve("mydb")
        val url = "jdbc:derby:" + db + ";create=true"
        val driver = "org.apache.derby.jdbc.EmbeddedDriver"

        val spec =
            s"""
              |connections:
              |  c0:
              |    kind: jdbc
              |    driver: $driver
              |    url: $url
              |relations:
              |  t0:
              |    kind: jdbc
              |    description: "This is a test table"
              |    connection: c0
              |    table: lala_001
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.getExecutor(project)
        val relation = project.relations("t0")

        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("SELECT * FROM lala_001"))
        }

        relation.create(executor)
        withDatabase(driver, url) { statement =>
            val result = statement.executeQuery("SELECT * FROM lala_001")
            val meta = result.getMetaData
            meta.getColumnName(1) should be ("str_col")
            meta.getColumnName(2) should be ("int_col")
            result.next() should be (false)
        }

        // TODO: Write records

        // TODO: Read records

        // TODO: Write modes (append, overwrite, ignore, fail)

        relation.destroy(executor)
        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("SELECT * FROM lala_001"))
        }
    }

    // TODO: Read /Write existing table without schema

    // TODO: Partitioned table
    //  * Create
    //  * Write modes (append, overwrite, ignore, fail)

}
