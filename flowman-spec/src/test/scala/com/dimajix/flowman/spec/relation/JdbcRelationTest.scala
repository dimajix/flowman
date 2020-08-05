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

package com.dimajix.flowman.spec.relation

import java.sql.Driver
import java.sql.DriverManager
import java.sql.Statement
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.testing.LocalSparkSession


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
        val executor = session.executor
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        val df = spark.createDataFrame(Seq(
            ("lala", 1),
            ("lolo", 2)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")

        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("SELECT * FROM lala_001"))
        }

        // == Create ===================================================================
        relation.exists(executor) should be (No)
        relation.exists(executor, Map()) should be (No)
        relation.create(executor)
        relation.exists(executor) should be (Yes)
        relation.exists(executor, Map()) should be (No)

        withDatabase(driver, url) { statement =>
            val result = statement.executeQuery("SELECT * FROM lala_001")
            val meta = result.getMetaData
            meta.getColumnName(1) should be ("STR_COL")
            meta.getColumnName(2) should be ("INT_COL")
            result.next() should be (false)
        }

        relation.read(executor, None).count() should be (0)

        // == Write ===================================================================
        // Write records
        relation.write(executor, df, mode=OutputMode.OVERWRITE)
        relation.exists(executor) should be (Yes)
        relation.exists(executor, Map()) should be (Yes)

        relation.read(executor, None).count() should be (2)

        // Append records
        relation.write(executor, df, mode=OutputMode.APPEND)
        relation.read(executor, None).count() should be (4)


        // Try write records
        relation.write(executor, df, mode=OutputMode.IGNORE_IF_EXISTS)
        relation.read(executor, None).count() should be (4)

        relation.truncate(executor)
        relation.read(executor, None).count() should be (0)

        relation.write(executor, df, mode=OutputMode.IGNORE_IF_EXISTS)
        relation.read(executor, None).count() should be (0)

        // Try write records
        an[Exception] shouldBe thrownBy(relation.write(executor, df, mode=OutputMode.ERROR_IF_EXISTS))
        relation.read(executor, None).count() should be (0)

        // == Truncate ===================================================================
        relation.truncate(executor)
        relation.exists(executor) should be (Yes)
        relation.exists(executor, Map()) should be (No)

        // == Destroy ===================================================================
        relation.destroy(executor)
        relation.exists(executor) should be (No)
        relation.exists(executor, Map()) should be (No)
        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("SELECT * FROM lala_001"))
        }
    }

    it should "support partitioned tables" in {
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
               |    partitions:
               |        - name: p_col
               |          type: integer
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        val df = spark.createDataFrame(Seq(
            ("lala", 1),
            ("lolo", 2)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")

        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("SELECT * FROM lala_001"))
        }

        // == Create ===================================================================
        relation.exists(executor) should be (No)
        relation.exists(executor, Map()) should be (No)
        relation.create(executor)
        relation.exists(executor) should be (Yes)
        relation.exists(executor, Map()) should be (No)

        withDatabase(driver, url) { statement =>
            val result = statement.executeQuery("SELECT * FROM lala_001")
            val meta = result.getMetaData
            meta.getColumnName(1) should be ("STR_COL")
            meta.getColumnName(2) should be ("INT_COL")
            meta.getColumnName(3) should be ("P_COL")
            result.next() should be (false)
        }

        relation.read(executor, None).count() should be (0)

        // == Write ===================================================================
        relation.write(executor, df, mode=OutputMode.OVERWRITE, partition=Map("p_col" -> SingleValue("1")))
        relation.exists(executor) should be (Yes)
        relation.exists(executor, Map("p_col" -> SingleValue("1"))) should be (Yes)
        relation.exists(executor, Map("p_col" -> SingleValue("2"))) should be (No)

        relation.read(executor, None).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        relation.write(executor, df, mode=OutputMode.OVERWRITE, partition=Map("p_col" -> SingleValue("1")))
        relation.read(executor, None).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        relation.write(executor, df, mode=OutputMode.OVERWRITE, partition=Map("p_col" -> SingleValue("2")))
        relation.read(executor, None).count() should be (4)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // Append records
        relation.write(executor, df, mode=OutputMode.APPEND, partition=Map("p_col" -> SingleValue("1")))
        relation.read(executor, None).count() should be (6)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (4)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // Try write records
        relation.write(executor, df, mode=OutputMode.IGNORE_IF_EXISTS, partition=Map("p_col" -> SingleValue("1")))
        relation.read(executor, None).count() should be (6)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (4)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // Try write records
        relation.write(executor, df, mode=OutputMode.IGNORE_IF_EXISTS, partition=Map("p_col" -> SingleValue("3")))
        relation.read(executor, None).count() should be (8)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (4)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("3"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // Try write records
        an[Exception] shouldBe thrownBy(relation.write(executor, df, mode=OutputMode.ERROR_IF_EXISTS))
        relation.read(executor, None).count() should be (8)

        // == Truncate ===================================================================
        relation.truncate(executor, Map("p_col" -> SingleValue("2")))
        relation.exists(executor) should be (Yes)
        relation.exists(executor, Map("p_col" -> SingleValue("1"))) should be (Yes)
        relation.exists(executor, Map("p_col" -> SingleValue("2"))) should be (No)

        relation.read(executor, None).count() should be (6)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (4)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (0)
        relation.read(executor, None, Map("p_col" -> SingleValue("3"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // Clean table
        relation.truncate(executor)
        relation.exists(executor) should be (Yes)
        relation.exists(executor, Map("p_col" -> SingleValue("1"))) should be (No)
        relation.exists(executor, Map("p_col" -> SingleValue("2"))) should be (No)
        relation.read(executor, None).count() should be (0)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (0)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (0)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Destroy ===================================================================
        relation.destroy(executor)
        relation.exists(executor) should be (No)
        relation.exists(executor, Map()) should be (No)
        relation.exists(executor, Map("p_col" -> SingleValue("1"))) should be (No)
        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("SELECT * FROM lala_001"))
        }
    }

    it should "support SQL queries" in {
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
               |    connection: c0
               |    table: lala_001
               |    schema:
               |      kind: inline
               |      fields:
               |        - name: str_col
               |          type: string
               |        - name: int_col
               |          type: integer
               |  t1:
               |    kind: jdbc
               |    connection: c0
               |    query: "SELECT * FROM lala_001"
               |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val relation_t0 = context.getRelation(RelationIdentifier("t0"))
        val relation_t1 = context.getRelation(RelationIdentifier("t1"))

        val df = spark.createDataFrame(Seq(
            ("lala", 1),
            ("lolo", 2)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")

        relation_t0.create(executor)
        relation_t0.write(executor, df, mode=OutputMode.OVERWRITE)

        // Spark up until 2.4.3 has problems with Derby
        if (spark.version > "2.4.3") {
            relation_t0.read(executor, None).count() should be(2)
            relation_t1.read(executor, None).count() should be(2)
        }

        relation_t0.destroy(executor)
    }
}
