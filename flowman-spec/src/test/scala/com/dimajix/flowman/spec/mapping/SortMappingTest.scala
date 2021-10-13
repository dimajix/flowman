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

package com.dimajix.flowman.spec.mapping

import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class SortMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "A SortMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  m1:
              |    kind: sort
              |    input: some_mapping
              |    columns:
              |      c1: ASC
              |      c2: desc nulls first
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val mapping = project.mappings("m1")
        mapping shouldBe a[SortMappingSpec]

        val instance = context.getMapping(MappingIdentifier("m1"))
        instance shouldBe a[SortMapping]

        val typedInstance = instance.asInstanceOf[SortMapping]
        typedInstance.input should be (MappingOutputIdentifier("some_mapping"))
        typedInstance.outputs should be (Seq("main"))
        typedInstance.columns should be (Seq(
            "c1" -> SortOrder(Ascending, NullsFirst),
            "c2" -> SortOrder(Descending, NullsFirst)
        ))
    }

    it should "work" in {
        val spark = this.spark
        import spark.implicits._
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val input = spark.range(10).repartition(10)
            .toDF()
            .withColumn("c1", col("id")%3)
            .withColumn("c2", col("id")%7)
        val inputSchema = StructType.of(input.schema)

        val mapping = SortMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input"),
            Seq(
                "c1" -> SortOrder(Ascending, NullsFirst),
                "id" -> SortOrder(Descending, NullsFirst)
            )
        )

        mapping.describe(executor, Map(MappingOutputIdentifier("input") -> inputSchema)) should be (Map("main" -> inputSchema))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("input") -> input))("main")
        result.as[(Long,Long,Long)].collect() should be (Seq(
            (9,0,2),
            (6,0,6),
            (3,0,3),
            (0,0,0),
            (7,1,0),
            (4,1,4),
            (1,1,1),
            (8,2,1),
            (5,2,5),
            (2,2,2)
        ))
    }
}
