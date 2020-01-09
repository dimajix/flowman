/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.flow

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.spark.testing.LocalSparkSession


class SqlMappingTest extends FlatSpec with Matchers with LocalSparkSession {

    "An multi line SQL Script" should "be readable from YML" in {
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

    "Another YAML multi line variant" should "be readable from YML" in {
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
        mapping.inputs should be (Seq(MappingOutputIdentifier("t0")))
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
        mapping.inputs.map(_.name).sorted should be (Seq("other_table", "some_table", "some_table_archive"))
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
        val executor = session.executor
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
    }

}
