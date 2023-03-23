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

package com.dimajix.flowman.spec.mapping

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.SPARK_VERSION
import com.dimajix.spark.testing.LocalSparkSession


class SqlMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {

    "The SQL Mapping" should "be readable from YML" in {
        val spec =
            """
              |mappings:
              |  t0:
              |    kind: provided
              |    table: my_table
              |
              |  t1:
              |    kind: sql
              |    sql: |
              |      SELECT x,y
              |
              |      FROM t0
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")

        project.mappings.size should be (2)
        project.mappings.contains("t0") should be (true)
        project.mappings.contains("t1") should be (true)
    }

    it should "be readable from another YML spec variant" in {
        val spec =
            """
              |mappings:
              |  t0:
              |    kind: provided
              |    table: my_table
              |
              |  t1:
              |    kind: sql
              |    sql: "
              |      SELECT x,y
              |
              |      FROM t0
              |      "
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")

        project.mappings.size should be (2)
        project.mappings.contains("t0") should be (true)
        project.mappings.contains("t1") should be (true)
    }

    it should "execute the SQL query" in {
        val spec =
            """
              |mappings:
              |  t1:
              |    kind: sql
              |    sql: "
              |      SELECT _1,_2
              |      FROM t0
              |      "
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.getContext(project)

        val df = executor.spark.createDataFrame(Seq(
            ("col1", 12),
            ("col2", 23)
        ))

        val mapping = context.getMapping(MappingIdentifier("t1"))
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("t0") -> df))("main")
            .orderBy("_1", "_2")
        result.schema should be (StructType(StructField("_1", StringType, true) :: StructField("_2", IntegerType, false) :: Nil))
        result.collect().size should be (2)

        mapping.describe(executor, Map(
            MappingOutputIdentifier("t0") -> com.dimajix.flowman.types.StructType.of(df.schema)
        )).map {case(k,v) => k -> v.sparkType } should be (Map(
            "main" -> result.schema
        ))

        session.shutdown()
    }

    it should "pass through descriptions" in {
        val sql =
            """
              |SELECT
              |     LOWER(CAST(x AS STRING)) AS y,
              |     TRUNC('HOUR', CAST(x AS DATE)) AS y2
              |FROM source
              |""".stripMargin

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val mapping = SqlMapping(
            Mapping.Properties(session.context),
            sql = Some(sql)
        )

        val inputSchema = ftypes.StructType(Seq(
            ftypes.Field("x", ftypes.StringType, description=Some("This is x"))
        ))
        val result = mapping.describe(execution, Map(MappingOutputIdentifier("source") -> inputSchema))
        result("main") should be (ftypes.StructType(Seq(
            ftypes.Field("y", ftypes.StringType, description=Some("This is x")),
            ftypes.Field("y2", ftypes.DateType, description=Some("This is x"))
        )))

        session.shutdown()
    }

    it should "support projects and outputs" in (if (SPARK_VERSION >= "3") {
        val sql =
            """
              |SELECT
              |     LOWER(CAST(_1 AS STRING)) AS y,
              |     CAST(_1 AS DATE) AS y2
              |FROM `proj/source:output`
              |""".stripMargin

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val mapping = SqlMapping(
            Mapping.Properties(session.context),
            sql = Some(sql)
        )

        mapping.inputs should be (Set(MappingOutputIdentifier("proj/source:output")))

        val df = execution.spark.createDataFrame(Seq(
            ("x", 12)
        ))
        val result = mapping.execute(execution, Map(MappingOutputIdentifier("proj/source:output") -> df))
        result("main").collect().size should be (1)

        val inputSchema = ftypes.StructType(Seq(
            ftypes.Field("_1", ftypes.StringType, description = Some("This is x"))
        ))
        val resultSchema = mapping.describe(execution, Map(MappingOutputIdentifier("proj/source:output") -> inputSchema))
        resultSchema("main") should be(ftypes.StructType(Seq(
            ftypes.Field("y", ftypes.StringType, description = Some("This is x")),
            ftypes.Field("y2", ftypes.DateType, description = Some("This is x"))
        )))

        session.shutdown()
    })

    "Dependencies" should "be correct" in {
        val spec =
            """
              |mappings:
              |  t1:
              |    kind: sql
              |    sql: "
              |      SELECT x,y
              |      FROM t0
              |      "
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        project.mappings.size should be (1)
        project.mappings.contains("t1") should be (true)

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val mapping = context.getMapping(MappingIdentifier("t1"))
        mapping.inputs should be (Set(MappingOutputIdentifier("t0")))

        session.shutdown()
    }

    it should "also be correct with subqueries" in {
        val spec =
            """
              |mappings:
              |  t1:
              |    kind: sql
              |    sql: "
              |          WITH current AS (
              |            SELECT
              |              0 AS from_archive,
              |              t.some_id
              |            FROM other_table other
              |            INNER JOIN some_table t ON t.some_id = other.id
              |          ),
              |          archive AS (
              |            SELECT
              |              1 AS from_archive,
              |              ta.some_id
              |            FROM other_table other
              |            INNER JOIN some_table_archive ta ON ta.some_id = other.id
              |          ),
              |          merged AS (
              |            SELECT
              |              COALESCE(cur.from_archive, ar.from_archive) AS from_archive,
              |              COALESCE(cur.other_id, ar.other_id) AS other_id
              |            FROM current cur
              |            FULL OUTER JOIN archive ar ON cur.some_id = ar.some_id AND cur.other_id = ar.other_id
              |          )
              |          SELECT
              |            from_archive,
              |            other_id
              |          FROM merged all
              |     "
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        project.mappings.size should be (1)
        project.mappings.contains("t1") should be (true)

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val mapping = context.getMapping(MappingIdentifier("t1"))
        mapping.inputs.map(_.name) should be (Set("other_table", "some_table", "some_table_archive"))

        session.shutdown()
    }
}
