/*
 * Copyright (C) 2018 The Flowman Authors
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

import com.dimajix.common.Yes
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.model.ConnectionReference
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.ValueConnectionReference
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.schema.InlineSchema
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.VarcharType
import com.dimajix.spark.testing.LocalSparkSession


class JdbcQueryRelationTest extends AnyFlatSpec with Matchers with LocalSparkSession {
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

    "The JdbcQueryRelation" should "support embedding the connection" in {
        val spec =
            s"""
               |kind: jdbcQuery
               |name: some_relation
               |description: "This is a test table"
               |sql: "SELECT * FROM some_table"
               |connection:
               |  kind: jdbc
               |  name: some_connection
               |  driver: some_driver
               |  url: some_url
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
            """.stripMargin
        val relationSpec = ObjectMapper.parse[RelationSpec](spec).asInstanceOf[JdbcQueryRelationSpec]

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context

        val relation = relationSpec.instantiate(context)
        relation.name should be ("some_relation")
        relation.provides(Operation.CREATE) should be (Set.empty)
        relation.requires(Operation.CREATE) should be (Set(ResourceIdentifier.ofJdbcTable("some_table", None)))
        relation.provides(Operation.WRITE) should be (Set.empty)
        relation.requires(Operation.WRITE) should be (Set.empty)
        relation.provides(Operation.READ) should be (Set(ResourceIdentifier.ofJdbcQuery("SELECT * FROM some_table")))
        relation.requires(Operation.READ) should be (Set(ResourceIdentifier.ofJdbcTablePartition("some_table", None, Map())))
        relation.schema should be (Some(InlineSchema(
                Schema.Properties(context, name="embedded", kind="inline"),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType),
                    Field("float_col", FloatType),
                    Field("varchar_col", VarcharType(10))
                )
            )))
        relation.fields should be (Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType),
            Field("float_col", FloatType),
            Field("varchar_col", VarcharType(10))
        ))
        relation.connection shouldBe a[ValueConnectionReference]
        relation.connection.identifier should be (ConnectionIdentifier("some_connection"))
        relation.connection.name should be ("some_connection")
        relation.primaryKey should be (Seq.empty)

        session.shutdown()
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

        val relation_t0 = JdbcTableRelation(
            Relation.Properties(context, "t0"),
            schema = Some(InlineSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType)
                )
            )),
            connection = ConnectionReference(context, ConnectionIdentifier("c0")),
            table = TableIdentifier("lala_004")
        )
        val relation_t1 = JdbcQueryRelation(
            Relation.Properties(context, "t1"),
            sql = Some("SELECT * FROM lala_004"),
            connection = ConnectionReference(context, ConnectionIdentifier("c0"))
        )

        val df = spark.createDataFrame(Seq(
                ("lala", 1),
                ("lolo", 2)
            ))
            .withColumnRenamed("_1", "str_col")
            .withColumnRenamed("_2", "int_col")

        relation_t1.provides(Operation.CREATE) should be (Set.empty)
        relation_t1.requires(Operation.CREATE) should be (Set(ResourceIdentifier.ofJdbcTable("lala_004", None)))
        relation_t1.provides(Operation.READ) should be (Set(ResourceIdentifier.ofJdbcQuery("SELECT * FROM lala_004")))
        relation_t1.requires(Operation.READ) should be (Set(ResourceIdentifier.ofJdbcTablePartition("lala_004", None, Map())))
        relation_t1.provides(Operation.WRITE) should be (Set.empty)
        relation_t1.requires(Operation.WRITE) should be (Set.empty)

        // == Create =================================================================================================
        relation_t0.create(execution)
        relation_t0.exists(execution) should be (Yes)
        relation_t0.conforms(execution) should be (Yes)
        relation_t1.exists(execution) should be (Yes)
        relation_t1.conforms(execution) should be (Yes)

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

        session.shutdown()
    }
}
