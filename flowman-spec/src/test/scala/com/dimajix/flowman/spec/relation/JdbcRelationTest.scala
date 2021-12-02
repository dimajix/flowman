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
import com.dimajix.flowman.model.ConnectionReference
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.ValueConnectionReference
import com.dimajix.flowman.spec.ObjectMapper
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

    "The JdbcRelation" should "support embedding the connection" in {
        val spec =
            s"""
               |kind: jdbc
               |name: some_relation
               |description: "This is a test table"
               |connection:
               |  kind: jdbc
               |  name: some_connection
               |  driver: some_driver
               |  url: some_url
               |table: lala_001
               |schema:
               |  kind: inline
               |  fields:
               |    - name: str_col
               |      type: string
               |    - name: int_col
               |      type: integer
            """.stripMargin
        val relationSpec = ObjectMapper.parse[RelationSpec](spec).asInstanceOf[JdbcRelationSpec]

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context

        val relation = relationSpec.instantiate(context)
        relation.name should be ("some_relation")
        relation.schema should be (Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType)
                )
            )))
        relation.connection shouldBe a[ValueConnectionReference]
        relation.connection.identifier should be (ConnectionIdentifier("some_connection"))
        relation.connection.name should be ("some_connection")
    }

    it should "support create" in {
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
              |          nullable: false
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
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

        relation.provides should be (Set(ResourceIdentifier.ofJdbcTable("lala_001", None)))
        relation.requires should be (Set())
        relation.resources() should be (Set(ResourceIdentifier.ofJdbcTablePartition("lala_001", None, Map())))

        // == Create ==================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (No)

        withDatabase(driver, url) { statement =>
            val result = statement.executeQuery("""SELECT * FROM LALA_001""")
            val meta = result.getMetaData
            meta.getColumnName(1) should be ("str_col")
            meta.getColumnName(2) should be ("int_col")
            result.next() should be (false)
        }

        relation.read(execution).count() should be (0)

        // == Write ===================================================================================================
        // Write records
        relation.write(execution, df, mode=OutputMode.OVERWRITE)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)

        relation.read(execution).count() should be (2)

        // Append records
        relation.write(execution, df, mode=OutputMode.APPEND)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.read(execution).count() should be (4)

        // Try write records
        relation.write(execution, df, mode=OutputMode.IGNORE_IF_EXISTS)
        relation.read(execution).count() should be (4)

        relation.truncate(execution)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.read(execution).count() should be (0)

        relation.write(execution, df, mode=OutputMode.IGNORE_IF_EXISTS)
        relation.read(execution).count() should be (0)

        // Try write records
        an[Exception] shouldBe thrownBy(relation.write(execution, df, mode=OutputMode.ERROR_IF_EXISTS))
        relation.read(execution).count() should be (0)

        // == Truncate ================================================================================================
        relation.truncate(execution)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (No)

        // == Destroy =================================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
        relation.loaded(execution, Map()) should be (No)
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
               |    table: lala_002
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
        val execution = session.execution
        val context = session.getContext(project)
        val relation = context.getRelation(RelationIdentifier("t0"))

        val df = spark.createDataFrame(Seq(
                ("lala", 1),
                ("lolo", 2)
            ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")

        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("""SELECT * FROM lala_002"""))
        }

        relation.provides should be (Set(ResourceIdentifier.ofJdbcTable("lala_002", None)))
        relation.requires should be (Set())
        relation.resources() should be (Set(ResourceIdentifier.ofJdbcTablePartition("lala_002", None, Map())))
        relation.resources(Map("p_col" -> SingleValue("23"))) should be (Set(ResourceIdentifier.ofJdbcTablePartition("lala_002", None, Map("p_col" -> "23"))))

        // == Create =================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.create(execution)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)

        withDatabase(driver, url) { statement =>
            val result = statement.executeQuery("""SELECT * FROM lala_002""")
            val meta = result.getMetaData
            meta.getColumnName(1) should be ("str_col")
            meta.getColumnName(2) should be ("int_col")
            meta.getColumnName(3) should be ("p_col")
            result.next() should be (false)
        }

        // == Read ===================================================================================================
        relation.read(execution).count() should be (0)

        // == Write ==================================================================================================
        relation.write(execution, df, mode=OutputMode.OVERWRITE, partition=Map("p_col" -> SingleValue("1")))
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("1"))) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Overwrite ==============================================================================================
        relation.write(execution, df, mode=OutputMode.OVERWRITE, partition=Map("p_col" -> SingleValue("1")))
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Write ==================================================================================================
        relation.write(execution, df, mode=OutputMode.OVERWRITE, partition=Map("p_col" -> SingleValue("2")))
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (4)
        relation.read(execution, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Append==================================================================================================
        relation.write(execution, df, mode=OutputMode.APPEND, partition=Map("p_col" -> SingleValue("1")))
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (6)
        relation.read(execution, Map("p_col" -> SingleValue("1"))).count() should be (4)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Try Write ==============================================================================================
        relation.write(execution, df, mode=OutputMode.IGNORE_IF_EXISTS, partition=Map("p_col" -> SingleValue("1")))
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (6)
        relation.read(execution, Map("p_col" -> SingleValue("1"))).count() should be (4)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Try Write ==============================================================================================
        relation.write(execution, df, mode=OutputMode.IGNORE_IF_EXISTS, partition=Map("p_col" -> SingleValue("3")))
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (8)
        relation.read(execution, Map("p_col" -> SingleValue("1"))).count() should be (4)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("3"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Try Write ==============================================================================================
        an[Exception] shouldBe thrownBy(relation.write(execution, df, mode=OutputMode.ERROR_IF_EXISTS))

        // == Read ===================================================================================================
        relation.read(execution).count() should be (8)

        // == Truncate ===============================================================================================
        relation.truncate(execution, Map("p_col" -> SingleValue("2")))
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("1"))) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (6)
        relation.read(execution, Map("p_col" -> SingleValue("1"))).count() should be (4)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (0)
        relation.read(execution, Map("p_col" -> SingleValue("3"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Truncate ===============================================================================================
        relation.truncate(execution)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("1"))) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (0)
        relation.read(execution, Map("p_col" -> SingleValue("1"))).count() should be (0)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (0)
        relation.read(execution, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Destroy ================================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("1"))) should be (No)
        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("SELECT * FROM lala_002"))
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
               |    table: lala_003
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
        val execution = session.execution
        val context = session.getContext(project)
        val relation = context.getRelation(RelationIdentifier("t0"))

        val df = spark.createDataFrame(Seq(
            ("lala", Some(1), 1),
            ("lolo", Some(2), 1),
            ("abc", Some(3), 2)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
            .withColumnRenamed("_3", "p_col")

        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("""SELECT * FROM lala_003"""))
        }

        // == Create =================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.create(execution)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)

        withDatabase(driver, url) { statement =>
            val result = statement.executeQuery("""SELECT * FROM lala_003""")
            val meta = result.getMetaData
            meta.getColumnName(1) should be ("str_col")
            meta.getColumnName(2) should be ("int_col")
            meta.getColumnName(3) should be ("p_col")
            result.next() should be (false)
        }

        // == Read ===================================================================================================
        relation.read(execution).count() should be (0)

        // == Write ==================================================================================================
        relation.write(execution, df, mode=OutputMode.OVERWRITE)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("1"))) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (Yes)
        relation.loaded(execution, Map("p_col" -> SingleValue("999"))) should be (No)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (3)
        relation.read(execution, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (1)
        relation.read(execution, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Append==================================================================================================
        relation.write(execution, df, mode=OutputMode.APPEND)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (6)
        relation.read(execution, Map("p_col" -> SingleValue("1"))).count() should be (4)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Overwrite ==============================================================================================
        relation.write(execution, df, mode=OutputMode.OVERWRITE)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (3)
        relation.read(execution, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (1)
        relation.read(execution, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Try Write ==============================================================================================
        relation.write(execution, df, mode=OutputMode.IGNORE_IF_EXISTS)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (3)
        relation.read(execution, Map("p_col" -> SingleValue("1"))).count() should be (2)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (1)
        relation.read(execution, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Try Write ==============================================================================================
        an[Exception] shouldBe thrownBy(relation.write(execution, df, mode=OutputMode.ERROR_IF_EXISTS))

        // == Truncate ===============================================================================================
        relation.truncate(execution)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("1"))) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("2"))) should be (No)

        // == Read ===================================================================================================
        relation.read(execution).count() should be (0)
        relation.read(execution, Map("p_col" -> SingleValue("1"))).count() should be (0)
        relation.read(execution, Map("p_col" -> SingleValue("2"))).count() should be (0)
        relation.read(execution, Map("p_col" -> SingleValue("999"))).count() should be (0)

        // == Destroy ================================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("p_col" -> SingleValue("1"))) should be (No)
        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("SELECT * FROM lala_003"))
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
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = Some("lala_004")
        )
        val relation_t1 = JdbcRelation(
            Relation.Properties(context, "t1"),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            query = Some("SELECT * FROM lala_004")
        )

        val df = spark.createDataFrame(Seq(
                ("lala", 1),
                ("lolo", 2)
            ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")

        relation_t1.provides should be (Set())
        relation_t1.requires should be (Set())
        relation_t1.resources() should be (Set(ResourceIdentifier.ofJdbcQuery("SELECT * FROM lala_004")))

        // == Create =================================================================================================
        relation_t0.create(execution)
        relation_t0.exists(execution) should be (Yes)
        relation_t0.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation_t0.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation_t1.exists(execution) should be (Yes)
        relation_t1.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation_t1.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Write ==================================================================================================
        relation_t0.write(execution, df, mode=OutputMode.OVERWRITE)

        // == Read ===================================================================================================
        // Spark up until 2.4.3 has problems with Derby
        if (spark.version > "2.4.3") {
            relation_t0.read(execution).count() should be(2)
            relation_t1.read(execution).count() should be(2)
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
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = Some("lala_005")
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
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = Some("lala_005")
        )

        // == Create =================================================================================================
        rel0.exists(execution) should be (No)
        rel0.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        rel0.conforms(execution, MigrationPolicy.STRICT) should be (No)
        rel0.create(execution)
        rel0.exists(execution) should be (Yes)
        rel0.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        rel0.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Read ===================================================================================================
        withConnection(url, "lala_005") { (con, options) =>
            JdbcUtils.getSchema(con, TableIdentifier("lala_005"), options)
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
        rel1.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        rel1.conforms(execution, MigrationPolicy.STRICT) should be (No)
        rel1.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.NEVER)
        rel0.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        rel0.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        rel1.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        rel1.conforms(execution, MigrationPolicy.STRICT) should be (No)
        a[MigrationFailedException] should be thrownBy(rel1.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.FAIL))
        a[MigrationFailedException] should be thrownBy(rel1.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.ALTER))
        a[MigrationFailedException] should be thrownBy(rel1.migrate(execution, MigrationPolicy.RELAXED, MigrationStrategy.ALTER))

        rel1.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        rel1.conforms(execution, MigrationPolicy.STRICT) should be (No)
        rel1.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.ALTER_REPLACE)
        rel0.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        rel0.conforms(execution, MigrationPolicy.STRICT) should be (No)
        rel1.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        rel1.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Read ===================================================================================================
        withConnection(url, "lala_005") { (con, options) =>
            JdbcUtils.getSchema(con, TableIdentifier("lala_005"), options)
        } should be (StructType(Seq(
            Field("int_col", DoubleType),
            Field("new_col", DateType)
        )))

        // == Destroy ===============================================================================================
        rel1.exists(execution) should be (Yes)
        rel1.destroy(execution)
        rel1.exists(execution) should be (No)
        rel1.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        rel1.conforms(execution, MigrationPolicy.STRICT) should be (No)
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
