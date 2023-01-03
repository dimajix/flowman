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
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class DistinctMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {

    "The DistinctMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  dedup:
              |    kind: distinct
              |    input: dummy
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)

        val mapping = project.mappings("dedup")
        mapping shouldBe a[DistinctMappingSpec]

        val instance = context.getMapping(MappingIdentifier("dedup"))
        instance shouldBe a[DistinctMapping]

        session.shutdown()
    }

    it should "work" in {
        val sparkSession = spark
        import sparkSession.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.context

        val mapping = DistinctMapping(
            Mapping.Properties(context),
            MappingOutputIdentifier("input")
        )

        val input = executor.spark.createDataFrame(Seq(
            Record("c1_v1", "c2_v1"),
            Record("c1_v1", "c2_v2"),
            Record("c1_v1", "c2_v2")
        ))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("input") -> input))("main")
        result.schema should be(input.schema)
        val rows = result.as[Record].collect()
        rows.size should be(2)

        session.shutdown()
    }

    it should "work with nested columns" in {
        val sparkSession = spark
        import sparkSession.implicits._

        val input = spark.read.json(Seq(
            """{"some_struct":{"f_1":12, "f_2":22},"other_struct":{"integer":13}}""",
            """{"some_struct":{"f_1":12, "f_2":22},"other_struct":{"integer":13}}""",
            """{"some_struct":{"f_1":12, "f_2":23},"other_struct":{"integer":13}}"""
        ).toDS)
        val inputSchema = StructType.of(input.schema)

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.context

        val mapping = DistinctMapping(
            Mapping.Properties(context),
            MappingOutputIdentifier("input")
        )

        mapping.describe(executor, Map(MappingOutputIdentifier("input") -> inputSchema)) should be (Map("main" -> inputSchema))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("input") -> input))("main")
        result.schema should be(input.schema)
        result.count should be(2)

        session.shutdown()
    }
}
