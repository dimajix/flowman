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

package com.dimajix.flowman.spec.relation

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.Statement
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.MigrationFailedException
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.jdbc.JdbcUtils
import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.spec.schema.EmbeddedSchema
import com.dimajix.flowman.types.DateType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class JdbcRelationTest extends AnyFlatSpec with Matchers with LocalSparkSession {
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
        val executor = session.execution
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        val df = spark.createDataFrame(Seq(
            ("lala", 1),
            ("lolo", 2)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")

        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("""SELECT * FROM lala_001"""))
        }

        // == Create ===================================================================
        relation.exists(executor) should be (No)
        relation.loaded(executor, Map()) should be (No)
        relation.create(executor)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map()) should be (No)

        withDatabase(driver, url) { statement =>
            val result = statement.executeQuery("""SELECT * FROM LALA_001""")
            val meta = result.getMetaData
            meta.getColumnName(1) should be ("str_col")
            meta.getColumnName(2) should be ("int_col")
            result.next() should be (false)
        }

        relation.read(executor, None).count() should be (0)

        // == Write ===================================================================
        // Write records
        relation.write(executor, df, mode=OutputMode.OVERWRITE)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map()) should be (Yes)

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
        relation.loaded(executor, Map()) should be (No)

        // == Destroy ===================================================================
        relation.destroy(executor)
        relation.exists(executor) should be (No)
        relation.loaded(executor, Map()) should be (No)
        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("""SELECT * FROM LALA_001"""))
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

        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val executor = session.execution
        val context = session.getContext(project)
        val relation = context.getRelation(RelationIdentifier("t0"))

        val df = spark.createDataFrame(Seq(
                ("lala", 1),
                ("lolo", 2)
            ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")

        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("""SELECT * FROM lala_001"""))
        }

        // == Create =================================================================================================
        relation.exists(executor) should be (No)
        relation.loaded(executor, Map()) should be (No)
        relation.create(executor)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map()) should be (No)

        withDatabase(driver, url) { statement =>
            val result = statement.executeQuery("""SELECT * FROM lala_001""")
            val meta = result.getMetaData
            meta.getColumnName(1) should be ("str_col")
            meta.getColumnName(2) should be ("int_col")
            meta.getColumnName(3) should be ("p_col")
            result.next() should be (false)
        }

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (0)

        // == Write ==================================================================================================
        relation.write(executor, df, mode=OutputMode.OVERWRITE, partition=Map("p_col" -> SingleValue("1")))
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map("p_col" -> SingleValue("1"))) should be (Yes)
        relation.loaded(executor, Map("p_col" -> SingleValue("2"))) should be (No)

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Overwrite ==============================================================================================
        relation.write(executor, df, mode=OutputMode.OVERWRITE, partition=Map("p_col" -> SingleValue("1")))

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Write ==================================================================================================
        relation.write(executor, df, mode=OutputMode.OVERWRITE, partition=Map("p_col" -> SingleValue("2")))

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (4)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Append==================================================================================================
        relation.write(executor, df, mode=OutputMode.APPEND, partition=Map("p_col" -> SingleValue("1")))

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (6)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (4)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Try Write ==============================================================================================
        relation.write(executor, df, mode=OutputMode.IGNORE_IF_EXISTS, partition=Map("p_col" -> SingleValue("1")))

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (6)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (4)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Try Write ==============================================================================================
        relation.write(executor, df, mode=OutputMode.IGNORE_IF_EXISTS, partition=Map("p_col" -> SingleValue("3")))

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (8)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (4)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("3"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Try Write ==============================================================================================
        an[Exception] shouldBe thrownBy(relation.write(executor, df, mode=OutputMode.ERROR_IF_EXISTS))

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (8)

        // == Truncate ===============================================================================================
        relation.truncate(executor, Map("p_col" -> SingleValue("2")))
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map()) should be (Yes)
        relation.loaded(executor, Map("p_col" -> SingleValue("1"))) should be (Yes)
        relation.loaded(executor, Map("p_col" -> SingleValue("2"))) should be (No)

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (6)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (4)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (0)
        relation.read(executor, None, Map("p_col" -> SingleValue("3"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Truncate ===============================================================================================
        relation.truncate(executor)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map()) should be (No)
        relation.loaded(executor, Map("p_col" -> SingleValue("1"))) should be (No)
        relation.loaded(executor, Map("p_col" -> SingleValue("2"))) should be (No)

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (0)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (0)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (0)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Destroy ================================================================================================
        relation.destroy(executor)
        relation.exists(executor) should be (No)
        relation.loaded(executor, Map()) should be (No)
        relation.loaded(executor, Map("p_col" -> SingleValue("1"))) should be (No)
        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("SELECT * FROM lala_001"))
        }
    }

    it should "support dynamically writing to partitioned tables" in {
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

        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val executor = session.execution
        val context = session.getContext(project)
        val relation = context.getRelation(RelationIdentifier("t0"))

        val df = spark.createDataFrame(Seq(
            ("lala", 1, 1),
            ("lolo", 2, 1),
            ("abc", 3, 2)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
            .withColumnRenamed("_3", "p_col")

        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("""SELECT * FROM lala_001"""))
        }

        // == Create =================================================================================================
        relation.exists(executor) should be (No)
        relation.loaded(executor, Map()) should be (No)
        relation.create(executor)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map()) should be (No)

        withDatabase(driver, url) { statement =>
            val result = statement.executeQuery("""SELECT * FROM lala_001""")
            val meta = result.getMetaData
            meta.getColumnName(1) should be ("str_col")
            meta.getColumnName(2) should be ("int_col")
            meta.getColumnName(3) should be ("p_col")
            result.next() should be (false)
        }

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (0)

        // == Write ==================================================================================================
        relation.write(executor, df, mode=OutputMode.OVERWRITE)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map("p_col" -> SingleValue("1"))) should be (Yes)
        relation.loaded(executor, Map("p_col" -> SingleValue("2"))) should be (Yes)
        relation.loaded(executor, Map("p_col" -> SingleValue("999"))) should be (No)

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (3)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (1)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Append==================================================================================================
        relation.write(executor, df, mode=OutputMode.APPEND)

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (6)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (4)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Overwrite ==============================================================================================
        relation.write(executor, df, mode=OutputMode.OVERWRITE)

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (3)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (1)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Try Write ==============================================================================================
        relation.write(executor, df, mode=OutputMode.IGNORE_IF_EXISTS)

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (3)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (1)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Try Write ==============================================================================================
        an[Exception] shouldBe thrownBy(relation.write(executor, df, mode=OutputMode.ERROR_IF_EXISTS))

        // == Truncate ===============================================================================================
        relation.truncate(executor)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map()) should be (No)
        relation.loaded(executor, Map("p_col" -> SingleValue("1"))) should be (No)
        relation.loaded(executor, Map("p_col" -> SingleValue("2"))) should be (No)

        // == Read ===================================================================================================
        relation.read(executor, None).count() should be (0)
        relation.read(executor, None, Map("p_col" -> SingleValue("1"))).count() should be (0)
        relation.read(executor, None, Map("p_col" -> SingleValue("2"))).count() should be (0)
        relation.read(executor, None, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Destroy ================================================================================================
        relation.destroy(executor)
        relation.exists(executor) should be (No)
        relation.loaded(executor, Map()) should be (No)
        relation.loaded(executor, Map("p_col" -> SingleValue("1"))) should be (No)
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
               |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.getContext(project)

        val relation_t0 = JdbcRelation(
            Relation.Properties(context, "t0"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType)
                )
            )),
            connection = ConnectionIdentifier("c0"),
            table = Some("lala_001")
        )
        val relation_t1 = JdbcRelation(
            Relation.Properties(context, "t1"),
            connection = ConnectionIdentifier("c0"),
            query = Some("SELECT * FROM lala_001")
        )

        val df = spark.createDataFrame(Seq(
                ("lala", 1),
                ("lolo", 2)
            ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")

        // == Create =================================================================================================
        relation_t0.create(execution)

        // == Write ==================================================================================================
        relation_t0.write(execution, df, mode=OutputMode.OVERWRITE)

        // == Read ===================================================================================================
        // Spark up until 2.4.3 has problems with Derby
        if (spark.version > "2.4.3") {
            relation_t0.read(execution, None).count() should be(2)
            relation_t1.read(execution, None).count() should be(2)
        }

        // == Destroy ================================================================================================
        relation_t0.destroy(execution)
    }

    it should "support migrations" in {
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
               |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.getContext(project)

        val rel0 = JdbcRelation(
            Relation.Properties(context, "t0"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType)
                )
            )),
            connection = ConnectionIdentifier("c0"),
            table = Some("lala_001")
        )
        val rel1 = JdbcRelation(
            Relation.Properties(context, "t1"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("int_col", DoubleType),
                    Field("new_col", DateType)
                )
            )),
            connection = ConnectionIdentifier("c0"),
            table = Some("lala_001")
        )

        // == Create =================================================================================================
        rel0.exists(execution) should be (No)
        rel0.create(execution)
        rel0.exists(execution) should be (Yes)

        // == Read ===================================================================================================
        withConnection(url, "lala_001") { (con, options) =>
            JdbcUtils.getSchema(con, TableIdentifier("lala_001"), options)
        } should be (StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        )))

        // == Migrate ===============================================================================================
        rel0.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.NEVER)
        rel0.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.FAIL)
        rel0.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER)
        rel0.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER_REPLACE)
        rel0.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.REPLACE)
        rel0.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.NEVER)
        rel0.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.FAIL)
        rel0.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.ALTER)
        rel0.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.ALTER_REPLACE)
        rel0.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.REPLACE)

        // == Migrate ===============================================================================================
        rel1.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.NEVER)
        a[MigrationFailedException] should be thrownBy(rel1.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.FAIL))
        a[MigrationFailedException] should be thrownBy(rel1.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.ALTER))
        rel1.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.ALTER_REPLACE)

        // == Read ===================================================================================================
        withConnection(url, "lala_001") { (con, options) =>
            JdbcUtils.getSchema(con, TableIdentifier("lala_001"), options)
        } should be (StructType(Seq(
            Field("int_col", DoubleType),
            Field("new_col", DateType)
        )))

        // == Destroy ===============================================================================================
        rel1.exists(execution) should be (Yes)
        rel1.destroy(execution)
        rel1.exists(execution) should be (No)
    }

    private def withConnection[T](url:String, table:String)(fn:(Connection,JDBCOptions) => T) : T = {
        val props = Map(
            JDBCOptions.JDBC_URL -> url,
            JDBCOptions.JDBC_DRIVER_CLASS -> "org.apache.derby.jdbc.EmbeddedDriver"
        )

        val options = new JDBCOptions(url, table, props)
        val conn = try {
            JdbcUtils.createConnection(options)
        } catch {
            case NonFatal(e) =>
                throw e
        }

        try {
            fn(conn, options)
        }
        finally {
            conn.close()
        }
    }
}
