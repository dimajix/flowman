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

package com.dimajix.flowman.spec.mapping

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.spec.mapping.DeduplicateMappingTest.Record
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


object DeduplicateMappingTest {
    case class Record(c1: String, c2: String)
}


class DeduplicateMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {

    "The DeduplicateMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  dedup:
              |    kind: deduplicate
              |    input: dummy
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val mapping = project.mappings("dedup")

        mapping shouldBe an[DeduplicateMappingSpec]

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)

        val instance = context.getMapping(MappingIdentifier("dedup"))
        instance should not be null
        instance shouldBe a[DeduplicateMapping]
    }

    it should "work without list of columns" in {
        val sparkSession = spark
        import sparkSession.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
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

        // Verify execution
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("input") -> input))("main")
        result.schema should be(input.schema)
        val rows = result.as[Record].collect()
        rows.size should be(1)

        // Verify schema
        val inputSchema = StructType(Seq(
            Field("c1", StringType),
            Field("c2", IntegerType, nullable = true)
        ))
        mapping.describe(executor, Map(MappingOutputIdentifier("input") -> inputSchema)) should be (Map("main" -> inputSchema))
    }

    it should "work with an explicit column list" in {
        val sparkSession = spark
        import sparkSession.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
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
        val executor = session.execution
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
