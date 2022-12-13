/*
 * Copyright 2022 Kaya Kupferschmidt
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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.common.Yes
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.model.ConnectionReference
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema.InlineSchema
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.VarcharType
import com.dimajix.spark.testing.LocalSparkSession


class DerbyJdbcViewRelationTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    implicit class JdbcViewRelationExt(rel: JdbcViewRelation) {
        def conforms(execution: Execution, policy: MigrationPolicy): Trilean = {
            rel.copy(migrationPolicy = policy).conforms(execution)
        }
        def migrate(execution: Execution, policy: MigrationPolicy, strategy: MigrationStrategy = MigrationStrategy.ALTER_REPLACE): Unit = {
            rel.copy(migrationPolicy = policy, migrationStrategy = strategy).migrate(execution)
        }
    }
    implicit class JdbcTableRelationExt(rel: JdbcTableRelation) {
        def conforms(execution: Execution, policy: MigrationPolicy): Trilean = {
            rel.copy(migrationPolicy = policy).conforms(execution)
        }
        def migrate(execution: Execution, policy: MigrationPolicy, strategy: MigrationStrategy = MigrationStrategy.ALTER_REPLACE): Unit = {
            rel.copy(migrationPolicy = policy, migrationStrategy = strategy).migrate(execution)
        }
    }


    private def db = tempDir.toPath.resolve("mydb")
    private def url = "jdbc:derby:" + db + ";create=true"
    private def driver = "org.apache.derby.jdbc.EmbeddedDriver"

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


    "The (Derby) JdbcViewRelation" should "support the full lifecycle" in {
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

        val tableRelation = JdbcTableRelation(
            Relation.Properties(context, "t0"),
            schema = Some(InlineSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType, nullable=false),
                    Field("varchar_col", VarcharType(10))
                )
            )),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = TableIdentifier("table_001")
        )
        tableRelation.create(execution)

        val df = spark.createDataFrame(Seq(
            ("lala", 1, "xyz"),
            ("lala", 2, "uvw"),  // TODO: null is not supported with Spark & Derby
            ("lolo", 2, "abc1234567890")
        ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")
            .withColumnRenamed("_3", "varchar_col")
        tableRelation.write(execution, df, mode=OutputMode.OVERWRITE)

        val viewRelation = JdbcViewRelation(
            Relation.Properties(context, "v0"),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            view = TableIdentifier("view_001"),
            sql = Some("SELECT * FROM table_001")
        )

        viewRelation.provides(Operation.CREATE) should be (Set(ResourceIdentifier.ofJdbcTable("view_001", None)))
        viewRelation.requires(Operation.CREATE) should be (Set(ResourceIdentifier.ofJdbcTable("table_001", None)))
        viewRelation.provides(Operation.READ) should be (Set(ResourceIdentifier.ofJdbcTablePartition("view_001", None, Map())))
        viewRelation.requires(Operation.READ) should be (Set(
            ResourceIdentifier.ofJdbcTable("view_001", None),
            ResourceIdentifier.ofJdbcTablePartition("table_001", None, Map())
        ))
        viewRelation.provides(Operation.WRITE) should be (Set(ResourceIdentifier.ofJdbcTablePartition("table_001", None, Map())))
        viewRelation.requires(Operation.WRITE) should be (Set(ResourceIdentifier.ofJdbcTable("view_001", None)))
        an[Exception] should be thrownBy(viewRelation.describe(execution))

        // == Create ==================================================================================================
        viewRelation.exists(execution) should be (No)
        viewRelation.loaded(execution, Map()) should be (No)
        viewRelation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        viewRelation.conforms(execution, MigrationPolicy.STRICT) should be (No)
        viewRelation.create(execution)
        viewRelation.exists(execution) should be (Yes)
        viewRelation.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        viewRelation.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        viewRelation.loaded(execution, Map()) should be (Yes)

        // == Read ====================================================================================================
        viewRelation.read(execution).count() should be (3)
        viewRelation.describe(execution, Map()) should be (StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType, nullable=false),
            Field("varchar_col", VarcharType(10))
        )))

        // == Destroy =================================================================================================
        viewRelation.destroy(execution)
        viewRelation.exists(execution) should be (No)
        viewRelation.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        viewRelation.conforms(execution, MigrationPolicy.STRICT) should be (No)
        viewRelation.loaded(execution, Map()) should be (No)

        tableRelation.destroy(execution)
    }

    it should "support migrations" in {
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

        val view1 = JdbcViewRelation(
            Relation.Properties(context, "v0"),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            view = TableIdentifier("view_001"),
            sql = Some("SELECT 1 AS a FROM (VALUES (1)) x")
        )
        val view2 = JdbcViewRelation(
            Relation.Properties(context, "v0"),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            view = TableIdentifier("view_001"),
            sql = Some("SELECT 2 AS a FROM (VALUES (1)) x")
        )

        view1.provides(Operation.CREATE) should be (Set(ResourceIdentifier.ofJdbcTable("view_001", None)))
        view1.requires(Operation.CREATE) should be (Set.empty)
        view1.provides(Operation.READ) should be (Set(ResourceIdentifier.ofJdbcTablePartition("view_001", None, Map())))
        view1.requires(Operation.READ) should be (Set(ResourceIdentifier.ofJdbcTable("view_001", None)))
        view1.provides(Operation.WRITE) should be (Set.empty)
        view1.requires(Operation.WRITE) should be (Set(ResourceIdentifier.ofJdbcTable("view_001", None)))

        // == Create ==================================================================================================
        view1.exists(execution) should be (No)
        view1.loaded(execution, Map()) should be (No)
        view1.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view1.conforms(execution, MigrationPolicy.STRICT) should be (No)
        view1.create(execution)
        view1.describe(execution, Map()) should be (StructType(Seq(
            Field("A", IntegerType, nullable=false)
        )))
        view1.exists(execution) should be (Yes)
        view1.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view1.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        view1.loaded(execution, Map()) should be (Yes)

        // == Read ====================================================================================================
        view1.read(execution).count() should be (1)

        // == Migrate =================================================================================================
        view2.exists(execution) should be (Yes)
        view2.loaded(execution, Map()) should be (Yes)
        view2.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view2.conforms(execution, MigrationPolicy.STRICT) should be (No)
        view2.migrate(execution, MigrationPolicy.RELAXED)
        view2.loaded(execution, Map()) should be (Yes)
        view2.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view2.conforms(execution, MigrationPolicy.STRICT) should be (Yes)

        // == Destroy =================================================================================================
        view1.destroy(execution)
        view1.exists(execution) should be (No)
        view1.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view1.conforms(execution, MigrationPolicy.STRICT) should be (No)
        view1.loaded(execution, Map()) should be (No)
    }

    it should "support migrating from a table to a view and vice versa" in {
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

        val view = JdbcViewRelation(
            Relation.Properties(context, "v0"),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            view = TableIdentifier("view_or_table"),
            sql = Some("SELECT 1 AS a FROM (VALUES (1)) x")
        )
        val table = JdbcTableRelation(
            Relation.Properties(context, "t0"),
            schema = Some(InlineSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType, nullable=false),
                    Field("varchar_col", VarcharType(10))
                )
            )),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = TableIdentifier("view_or_table")
        )

        // == Create VIEW ============================================================================================
        view.exists(execution) should be (No)
        view.loaded(execution, Map()) should be (No)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view.conforms(execution, MigrationPolicy.STRICT) should be (No)
        view.create(execution)

        view.exists(execution) should be (Yes)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        view.loaded(execution, Map()) should be (Yes)

        table.exists(execution) should be (Yes)
        table.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        table.conforms(execution, MigrationPolicy.STRICT) should be (No)
        table.loaded(execution, Map()) should be (Yes)

        // == Migrate to TABLE =======================================================================================
        table.migrate(execution, MigrationPolicy.RELAXED)

        view.exists(execution) should be (Yes)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view.conforms(execution, MigrationPolicy.STRICT) should be (No)
        view.loaded(execution, Map()) should be (Yes)

        table.exists(execution) should be (Yes)
        table.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        table.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        table.loaded(execution, Map()) should be (No)

        // == Migrate to VIEW ========================================================================================
        view.migrate(execution, MigrationPolicy.RELAXED)

        view.exists(execution) should be (Yes)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (Yes)
        view.conforms(execution, MigrationPolicy.STRICT) should be (Yes)
        view.loaded(execution, Map()) should be (Yes)

        table.exists(execution) should be (Yes)
        table.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        table.conforms(execution, MigrationPolicy.STRICT) should be (No)
        table.loaded(execution, Map()) should be (Yes)

        // == Destroy =================================================================================================
        view.destroy(execution)
        view.exists(execution) should be (No)
        view.conforms(execution, MigrationPolicy.RELAXED) should be (No)
        view.conforms(execution, MigrationPolicy.STRICT) should be (No)
        view.loaded(execution, Map()) should be (No)
    }
}
