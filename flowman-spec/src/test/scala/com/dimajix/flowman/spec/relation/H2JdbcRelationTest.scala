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

package com.dimajix.flowman.spec.relation

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.Statement
import java.util.Properties

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructField
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableIndex
import com.dimajix.flowman.catalog.TableType
import com.dimajix.flowman.execution.DeleteClause
import com.dimajix.flowman.execution.InsertClause
import com.dimajix.flowman.execution.MigrationFailedException
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.UpdateClause
import com.dimajix.flowman.jdbc.JdbcUtils
import com.dimajix.flowman.jdbc.SqlDialects
import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.model.ConnectionReference
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.ValueConnectionReference
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.schema.EmbeddedSchema
import com.dimajix.flowman.types.DateType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.VarcharType
import com.dimajix.spark.sql.DataFrameBuilder
import com.dimajix.spark.testing.LocalSparkSession


class H2JdbcRelationTest extends AnyFlatSpec with Matchers with LocalSparkSession {
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

    "The (H2) JdbcTableRelation" should "support embedding the connection" in {
        val spec =
            s"""
               |kind: jdbcTable
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
               |    - name: float_col
               |      type: float
               |    - name: varchar_col
               |      type: varchar(10)
               |  primaryKey:
               |    - int_col
               |indexes:
               |  - name: idx0
               |    columns: [str_col, int_col]
               |    unique: false
               |primaryKey:
               |  - str_col
            """.stripMargin
        val relationSpec = ObjectMapper.parse[RelationSpec](spec).asInstanceOf[JdbcTableRelationSpec]

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context

        val relation = relationSpec.instantiate(context)
        relation.name should be ("some_relation")
        relation.schema should be (Some(EmbeddedSchema(
                Schema.Properties(context, name="embedded", kind="inline"),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType),
                    Field("float_col", FloatType),
                    Field("varchar_col", VarcharType(10))
                ),
                primaryKey = Seq("int_col")
            )))
        relation.fields should be (Seq(
            Field("str_col", StringType, nullable=false),
            Field("int_col", IntegerType),
            Field("float_col", FloatType),
            Field("varchar_col", VarcharType(10))
        ))
        relation.connection shouldBe a[ValueConnectionReference]
        relation.connection.identifier should be (ConnectionIdentifier("some_connection"))
        relation.connection.name should be ("some_connection")
        relation.indexes should be (Seq(TableIndex("idx0", Seq("str_col", "int_col"))))
        relation.primaryKey should be (Seq("str_col"))
    }

    it should "support the full lifecycle" in {
        val db = tempDir.toPath.resolve("mydb")
        val url = "jdbc:h2:" + db
        val driver = "org.h2.Driver"

        val spec =
            s"""
              |connections:
              |  c0:
              |    kind: jdbc
              |    driver: $driver
              |    url: $url
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.getContext(project)

        val relation = JdbcTableRelation(
            Relation.Properties(context, "t0"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType, nullable=false),
                    Field("varchar_col", VarcharType(10))
                )
            )),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = TableIdentifier("lala_001")
        )

        val df = spark.createDataFrame(Seq(
            ("lala", 1, "xyz"),
            ("lala", 1, null),
            ("lolo", 2, "abc1234567890")
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
            .withColumnRenamed("_3", "varchar_col")

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
            meta.getColumnName(1) should be ("STR_COL")
            meta.getColumnName(2) should be ("INT_COL")
            meta.getColumnName(3) should be ("VARCHAR_COL")

            val dialect = SqlDialects.get(url)
            val schema = JdbcUtils.getSchema(result, dialect)
            schema should be (StructType(Seq(
                Field("STR_COL", StringType),
                Field("INT_COL", IntegerType, nullable=false),
                Field("VARCHAR_COL", VarcharType(10))
            )))

            result.next() should be (false)
            result.close()
        }

        relation.read(execution).count() should be (0)

        // == Write ===================================================================================================
        // Write records
        relation.write(execution, df, mode=OutputMode.OVERWRITE)
        relation.exists(execution) should be (Yes)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)

        relation.read(execution).count() should be (3)

        // Append records
        relation.write(execution, df, mode=OutputMode.APPEND)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.read(execution).count() should be (6)

        // Try write records
        relation.write(execution, df, mode=OutputMode.IGNORE_IF_EXISTS)
        relation.read(execution).count() should be (6)

        relation.truncate(execution)
        relation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        relation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        relation.read(execution).count() should be (0)

        relation.write(execution, df, mode=OutputMode.IGNORE_IF_EXISTS)
        relation.read(execution).count() should be (3)

        // Try write records
        an[Exception] shouldBe thrownBy(relation.write(execution, df, mode=OutputMode.ERROR_IF_EXISTS))
        relation.read(execution).count() should be (3)

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

    it should "support the full lifecycle with a staging table" in {
        val db = tempDir.toPath.resolve("mydb")
        val url = "jdbc:h2:" + db
        val driver = "org.h2.Driver"

        val spec =
            s"""
               |connections:
               |  c0:
               |    kind: jdbc
               |    driver: $driver
               |    url: $url
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.getContext(project)

        val relation = JdbcTableRelation(
            Relation.Properties(context, "t0"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType),
                    Field("varchar_col", VarcharType(10))
                )
            )),
            partitions = Seq(PartitionField("part", IntegerType)),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = TableIdentifier("lala_001"),
            stagingTable = Some(TableIdentifier("lala_001_staging"))
        )

        val df = spark.createDataFrame(Seq(
            ("lala", 1, "xyz", 2),
            ("lolo", 2, "abc1234567890", 3)
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
            .withColumnRenamed("_3", "varchar_col")
            .withColumnRenamed("_4", "part")

        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("""SELECT * FROM lala_001"""))
        }

        // == Create ==================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)

        withDatabase(driver, url) { statement =>
            val result = statement.executeQuery("""SELECT * FROM LALA_001""")
            val meta = result.getMetaData
            meta.getColumnName(1) should be ("STR_COL")
            meta.getColumnName(2) should be ("INT_COL")
            meta.getColumnName(3) should be ("VARCHAR_COL")
            meta.getColumnName(4) should be ("PART")

            val dialect = SqlDialects.get(url)
            val schema = JdbcUtils.getSchema(result, dialect)
            schema should be (StructType(Seq(
                Field("STR_COL", StringType),
                Field("INT_COL", IntegerType),
                Field("VARCHAR_COL", VarcharType(10)),
                Field("PART", IntegerType, nullable=false)
            )))

            result.next() should be (false)
            result.close()
        }

        relation.read(execution).count() should be (0)

        // == Write ===================================================================================================
        // Write records
        relation.write(execution, df, mode=OutputMode.OVERWRITE)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)

        relation.read(execution).count() should be (2)

        // Append records
        relation.write(execution, df, mode=OutputMode.APPEND)
        relation.read(execution).count() should be (4)

        // Try write records
        relation.write(execution, df, mode=OutputMode.IGNORE_IF_EXISTS)
        relation.read(execution).count() should be (4)

        relation.truncate(execution)
        relation.read(execution).count() should be (0)

        relation.write(execution, df, mode=OutputMode.IGNORE_IF_EXISTS)
        relation.read(execution).count() should be (2)

        // Try write records
        an[Exception] shouldBe thrownBy(relation.write(execution, df, mode=OutputMode.ERROR_IF_EXISTS))
        relation.read(execution).count() should be (2)

        // Overwrite partition
        relation.write(execution, df.select("str_col", "int_col"), mode=OutputMode.OVERWRITE, partition = Map("part" -> SingleValue("12")))
        relation.read(execution).count() should be (4)
        relation.write(execution, df.select("str_col", "int_col"), mode=OutputMode.OVERWRITE, partition = Map("part" -> SingleValue("12")))
        relation.read(execution).count() should be (4)

        // == Truncate ================================================================================================
        relation.truncate(execution)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)

        // == Destroy =================================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        withDatabase(driver, url) { statement =>
            an[Exception] shouldBe thrownBy(statement.executeQuery("""SELECT * FROM LALA_001"""))
        }
    }

    it should "support partitioned tables" in {
        val db = tempDir.toPath.resolve("mydb")
        val url = "jdbc:h2:" + db
        val driver = "org.h2.Driver"

        val spec =
            s"""
               |connections:
               |  c0:
               |    kind: jdbc
               |    driver: $driver
               |    url: $url
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.getContext(project)
        val relation = JdbcTableRelation(
            Relation.Properties(context, "t0"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType)
                )
            )),
            partitions = Seq(PartitionField("p_col", IntegerType)),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = TableIdentifier("lala_002")
        )

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
            meta.getColumnName(1) should be ("STR_COL")
            meta.getColumnName(2) should be ("INT_COL")
            meta.getColumnName(3) should be ("P_COL")

            val dialect = SqlDialects.get(url)
            val schema = JdbcUtils.getSchema(result, dialect)
            schema should be (StructType(Seq(
                Field("STR_COL", StringType),
                Field("INT_COL", IntegerType),
                Field("P_COL", IntegerType, nullable=false)
            )))

            result.next() should be (false)
            result.close()
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
        val url = "jdbc:h2:" + db
        val driver = "org.h2.Driver"

        val spec =
            s"""
               |connections:
               |  c0:
               |    kind: jdbc
               |    driver: $driver
               |    url: $url
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.getContext(project)
        val relation = JdbcTableRelation(
            Relation.Properties(context, "t0"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType)
                )
            )),
            partitions = Seq(PartitionField("p_col", IntegerType)),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = TableIdentifier("lala_003")
        )

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
            meta.getColumnName(1) should be ("STR_COL")
            meta.getColumnName(2) should be ("INT_COL")
            meta.getColumnName(3) should be ("P_COL")

            val dialect = SqlDialects.get(url)
            val schema = JdbcUtils.getSchema(result, dialect)
            schema should be (StructType(Seq(
                Field("STR_COL", StringType),
                Field("INT_COL", IntegerType),
                Field("P_COL", IntegerType, nullable=false)
            )))

            result.next() should be (false)
            result.close()
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

    it should "support merge operations with complex clauses" in {
        val db = tempDir.toPath.resolve("mydb")
        val url = "jdbc:h2:" + db
        val driver = "org.h2.Driver"

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

        val relation = JdbcTableRelation(
            Relation.Properties(context, "t0"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("id", IntegerType),
                    Field("name", StringType),
                    Field("sex", StringType),
                    Field("state", VarcharType(20))
                )
            )),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = TableIdentifier("lala_001")
        )

        // == Create ==================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.read(execution).count() should be (0)

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
        relation.write(execution, df0, mode=OutputMode.APPEND)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)

        // ===== Read Table ===========================================================================================
        val df1 = relation.read(execution)
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
        relation.merge(execution, df2, Some(expr("source.id = target.id")), clauses)

        // ===== Read Table ===========================================================================================
        val df3 = relation.read(execution)
        df3.sort(col("id")).collect() should be (Seq(
            Row(10, "Alice", "female", "mutable"),
            Row(20, "Bob", "male", "immutable"),
            Row(40, "Eve", "female", "immutable"),
            Row(50, "Debora", "female", "mutable")
        ))

        // == Destroy =================================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
    }

    it should "support merge operations with complex clauses and staging tables" in {
        val db = tempDir.toPath.resolve("mydb")
        val url = "jdbc:h2:" + db
        val driver = "org.h2.Driver"

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

        val relation = JdbcTableRelation(
            Relation.Properties(context, "t0"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("id", IntegerType),
                    Field("name", StringType),
                    Field("sex", VarcharType(20)),
                    Field("state", VarcharType(20))
                )
            )),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = TableIdentifier("lala_001"),
            stagingTable = Some(TableIdentifier("lala_001_staging"))
        )

        // == Create ==================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.read(execution).count() should be (0)

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
        relation.write(execution, df0, mode=OutputMode.APPEND)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)

        // ===== Read Table ===========================================================================================
        val df1 = relation.read(execution)
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
        relation.merge(execution, df2, Some(expr("source.id = target.id")), clauses)

        // ===== Read Table ===========================================================================================
        val df3 = relation.read(execution)
        df3.sort(col("id")).collect() should be (Seq(
            Row(10, "Alice", "female", "mutable"),
            Row(20, "Bob", "male", "immutable"),
            Row(40, "Eve", "female", "immutable"),
            Row(50, "Debora", "female", "mutable")
        ))

        // == Destroy =================================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
    }

    it should "support merge operations with trivial clauses" in {
        val db = tempDir.toPath.resolve("mydb")
        val url = "jdbc:h2:" + db
        val driver = "org.h2.Driver"

        val spec =
            s"""
               |connections:
               |  c0:
               |    kind: jdbc
               |    driver: $driver
               |    url: $url
               |relations:
               |  t0:
               |    kind: jdbcTable
               |    description: "This is a test table"
               |    connection: c0
               |    table: lala_001
               |    schema:
               |      kind: inline
               |      fields:
               |        - name: id
               |          type: integer
               |        - name: name
               |          type: string
               |        - name: sex
               |          type: string
               |      primaryKey: ID
               |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        // == Create ==================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.read(execution).count() should be (0)

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
        relation.write(execution, df0, mode=OutputMode.APPEND)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)

        // ===== Read Table ===========================================================================================
        val df1 = relation.read(execution)
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
        val clauses = Seq(
            InsertClause(),
            UpdateClause()
        )
        relation.merge(execution, df2, None, clauses)

        // ===== Read Table ===========================================================================================
        val df3 = relation.read(execution)
        df3.sort(col("id")).collect() should be (Seq(
            Row(10, "Alice", "female"),
            Row(20, "Bob", "male"),
            Row(50, "Debora", "female")
        ))

        // == Destroy =================================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
    }

    it should "support upsert operations" in {
        val db = tempDir.toPath.resolve("mydb")
        val url = "jdbc:h2:" + db
        val driver = "org.h2.Driver"

        val spec =
            s"""
               |connections:
               |  c0:
               |    kind: jdbc
               |    driver: $driver
               |    url: $url
               |relations:
               |  t0:
               |    kind: jdbcTable
               |    description: "This is a test table"
               |    connection: c0
               |    table: lala_001
               |    schema:
               |      kind: inline
               |      fields:
               |        - name: id
               |          type: integer
               |        - name: name
               |          type: string
               |        - name: sex
               |          type: varchar(10)
               |      primaryKey: ID
               |""".stripMargin
        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        // == Create ==================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.create(execution)
        relation.exists(execution) should be (Yes)
        relation.read(execution).count() should be (0)

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
        relation.write(execution, df0, mode=OutputMode.APPEND)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)

        // ===== Read Table ===========================================================================================
        val df1 = relation.read(execution)
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
        relation.write(execution, df2, mode=OutputMode.UPDATE)

        // ===== Read Table ===========================================================================================
        val df3 = relation.read(execution)
        df3.sort(col("id")).collect() should be (Seq(
            Row(10, "Alice", "female"),
            Row(20, "Bob", "male"),
            Row(50, "Debora", "female")
        ))

        // == Destroy =================================================================================================
        relation.destroy(execution)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
    }

    it should "support migrations" in {
        val db = tempDir.toPath.resolve("mydb")
        val url = "jdbc:h2:" + db
        val driver = "org.h2.Driver"

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

        val rel0 = JdbcTableRelation(
            Relation.Properties(context, "t0"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType)
                )
            )),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = TableIdentifier("lala_005")
        )
        val rel1 = JdbcTableRelation(
            Relation.Properties(context, "t1"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("int_col", DoubleType),
                    Field("new_col", DateType)
                )
            )),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = TableIdentifier("lala_005")
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
            JdbcUtils.getTableSchema(con, TableIdentifier("lala_005"), options)
        } should be (StructType(Seq(
            Field("STR_COL", StringType),
            Field("INT_COL", IntegerType)
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
        rel1.migrate(execution, MigrationPolicy.STRICT, MigrationStrategy.ALTER)
        rel0.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        rel0.conforms(execution, MigrationPolicy.STRICT) should be (No)
        rel1.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        rel1.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Read ===================================================================================================
        withConnection(url, "lala_005") { (con, options) =>
            JdbcUtils.getTableSchema(con, TableIdentifier("lala_005"), options)
        } should be (StructType(Seq(
            Field("INT_COL", DoubleType),
            Field("NEW_COL", DateType)
        )))

        // == Destroy ===============================================================================================
        rel1.exists(execution) should be (Yes)
        rel1.destroy(execution)
        rel1.exists(execution) should be (No)
        rel1.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        rel1.conforms(execution, MigrationPolicy.STRICT) should be (No)
    }

    it should "support a primary key" in {
        val db = tempDir.toPath.resolve("mydb")
        val url = "jdbc:h2:" + db
        val driver = "org.h2.Driver"

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

        val rel0 = JdbcTableRelation(
            Relation.Properties(context, "t0"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType),
                    Field("varchar_col", VarcharType(32))
                )
            )),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = TableIdentifier("lala_006"),
            primaryKey = Seq("int_col", "varchar_col")
        )

        // == Create ==================================================================================================
        rel0.exists(execution) should be (No)
        rel0.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        rel0.conforms(execution, MigrationPolicy.STRICT) should be (No)
        rel0.create(execution)
        rel0.exists(execution) should be (Yes)
        rel0.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        rel0.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Inspect =================================================================================================
        withConnection(url, "lala_006") { (con, options) =>
            JdbcUtils.getTable(con, TableIdentifier("lala_006"), options)
        } should be (
            TableDefinition(
                TableIdentifier("lala_006"),
                TableType.TABLE,
                columns = Seq(
                    Field("STR_COL", StringType),
                    Field("INT_COL", IntegerType, nullable=false),
                    Field("VARCHAR_COL", VarcharType(32), nullable=false)
                ),
                primaryKey = Seq("INT_COL", "VARCHAR_COL")
            ))

        // == Destroy =================================================================================================
        rel0.exists(execution) should be (Yes)
        rel0.destroy(execution)
        rel0.exists(execution) should be (No)
    }

    it should "support indexes" in {
        val db = tempDir.toPath.resolve("mydb")
        val url = "jdbc:h2:" + db
        val driver = "org.h2.Driver"

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

        val rel0 = JdbcTableRelation(
            Relation.Properties(context, "t0"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType),
                    Field("varchar_col", VarcharType(32))
                )
            )),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = TableIdentifier("lala_007"),
            indexes = Seq(TableIndex("idx0",Seq("int_col", "varchar_col")))
        )

        // == Create ==================================================================================================
        rel0.exists(execution) should be (No)
        rel0.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        rel0.conforms(execution, MigrationPolicy.STRICT) should be (No)
        rel0.create(execution)
        rel0.exists(execution) should be (Yes)
        rel0.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        rel0.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Inspect =================================================================================================
        withConnection(url, "lala_007") { (con, options) =>
            JdbcUtils.getTable(con, TableIdentifier("lala_007"), options)
        } should be (
            TableDefinition(
                TableIdentifier("lala_007"),
                TableType.TABLE,
                columns = Seq(
                    Field("STR_COL", StringType),
                    Field("INT_COL", IntegerType),
                    Field("VARCHAR_COL", VarcharType(32))
                ),
                indexes = Seq(TableIndex("IDX0",Seq("INT_COL", "VARCHAR_COL")))
        ))

        // == Destroy =================================================================================================
        rel0.exists(execution) should be (Yes)
        rel0.destroy(execution)
        rel0.exists(execution) should be (No)
    }

    private def withConnection[T](url:String, table:String)(fn:(Connection,JDBCOptions) => T) : T = {
        val props = Map(
            JDBCOptions.JDBC_URL -> url,
            JDBCOptions.JDBC_DRIVER_CLASS -> "org.h2.Driver"
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
