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

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.flow.DeduplicateMappingTest.Record
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


object DeduplicateMappingTest {
    case class Record(c1: String, c2: String)
}


class DeduplicateMappingTest extends FlatSpec with Matchers with LocalSparkSession {

    "The DeduplicateMapping" should "work without list of columns" in {
        val sparkSession = spark
        import sparkSession.implicits._

        val spec =
            """
              |mappings:
              |  dummy:
              |    kind: provided
              |    table: my_table
              |  dedup:
              |    kind: deduplicate
              |    input: dummy
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.mappings.keys should contain("dedup")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        executor.spark.createDataFrame(Seq(
            Record("c1_v1", "c2_v1"),
            Record("c1_v1", "c2_v2"),
            Record("c1_v1", "c2_v2")
        )).createOrReplaceTempView("my_table")

        val mapping = context.getMapping(MappingIdentifier("dedup"))
        mapping should not be null

        val df = executor.instantiate(mapping, "main")
        val rows = df.as[Record].collect()
        rows.size should be(2)

        // Verify schema
        val inputSchema =  StructType(Seq(
            Field("c1", StringType),
            Field("c2", IntegerType, nullable = true)
        ))
        mapping.describe(executor, Map(MappingOutputIdentifier("dummy") -> inputSchema)) should be (Map("main" -> inputSchema))
    }

    it should "work with an explicit column list" in {
        val sparkSession = spark
        import sparkSession.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.context

        val mapping = DeduplicateMapping(
            Mapping.Properties(context),
            MappingOutputIdentifier("input"),
            Seq("c1")
        )

        val input = executor.spark.createDataFrame(Seq(
            Record("c1_v1", "c2_v1"),
            Record("c1_v1", "c2_v2"),
            Record("c1_v1", "c2_v2")
        ))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("input") -> input))("main")
        result.schema should be(input.schema)
        val rows = result.as[Record].collect()
        rows.size should be(1)
    }

    it should "work with nested columns" in {
        val sparkSession = spark
        import sparkSession.implicits._

        val input = spark.read.json(Seq(
            """{"some_struct":{"f_1":12, "f_2":22},"other_struct":{"integer":13}}""",
            """{"some_struct":{"f_1":12, "f_2":23},"other_struct":{"integer":15}}""",
            """{"some_struct":{"f_1":13, "f_2":23},"other_struct":{"integer":15}}"""
        ).toDS)

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.context

        val mapping = DeduplicateMapping(
            Mapping.Properties(context),
            MappingOutputIdentifier("input"),
            Seq("some_struct.f_1")
        )

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("input") -> input))("main")
        result.schema should be(input.schema)
        val rows = result.collect()
        rows.size should be(2)
    }
}
