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

import org.apache.spark.sql.Row
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.testing.LocalSparkSession
import com.dimajix.flowman.transforms.ProjectTransformer
import com.dimajix.flowman.transforms.schema.Path
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType


class ProjectMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    "The ProjectMapping" should "work" in {
        val df = spark.createDataFrame(Seq(
            ("col1", 12),
            ("col2", 23)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = ProjectMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("myview"),
            Seq(ProjectTransformer.Column(Path("_2")))
        )

        mapping.input should be (MappingOutputIdentifier("myview"))
        mapping.columns should be (Seq(ProjectTransformer.Column(Path("_2"))))
        mapping.dependencies should be (Seq(MappingOutputIdentifier("myview")))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("myview") -> df))("main")
            .orderBy("_2").collect()
        result.size should be (2)
        result(0) should be (Row(12))
        result(1) should be (Row(23))

        val schema = mapping.describe(Map(MappingOutputIdentifier("myview") -> StructType.of(df.schema)))
        schema("main") should be (
            StructType(Seq(
                Field("_2", IntegerType, false)
            ))
        )
    }

    "An appropriate Dataflow" should "be readable from YML" in {
        val spec =
            """
              |mappings:
              |  t1:
              |    kind: project
              |    input: t0
              |    columns:
              |      - _2
              |      - _1
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        project.mappings.size should be (1)
        project.mappings.contains("t0") should be (false)
        project.mappings.contains("t1") should be (true)

        val df = spark.createDataFrame(Seq(
            ("col1", 12),
            ("col2", 23)
        ))

        val expectedSchema = StructType(Seq(
            Field("_2", IntegerType, false),
            Field("_1", StringType, true)
        ))

        val mapping = project.mappings("t1").instantiate(session.context)
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("t0") -> df))("main")
        result.schema should be (expectedSchema.sparkType)

        val schema = mapping.describe(Map(MappingOutputIdentifier("t0") -> StructType.of(df.schema)))
        schema("main") should be (expectedSchema)
    }

    it should "support renaming and retyping" in {
        val spec =
            """
              |mappings:
              |  t1:
              |    kind: project
              |    input: t0
              |    columns:
              |      - name: second
              |        column: _2
              |        type: Long
              |      - name: first
              |        column: _1
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val df = spark.createDataFrame(Seq(
            ("col1", 12),
            ("col2", 23)
        ))

        val expectedSchema = StructType(Seq(
            Field("second", LongType, false),
            Field("first", StringType, true)
        ))

        val mapping = project.mappings("t1").instantiate(session.context)
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("t0") -> df))("main")
        result.schema should be (expectedSchema.sparkType)

        val schema = mapping.describe(Map(MappingOutputIdentifier("t0") -> StructType.of(df.schema)))
        schema("main") should be (expectedSchema)
    }

    it should "support nested columns" in {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val spec =
            """
              |mappings:
              |  t1:
              |    kind: project
              |    input: t0
              |    columns:
              |      - name: my_field
              |        column: some_struct.field
              |        type: Long
              |      - other_struct.integer
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")

        val df = spark.read.json(Seq(
            """{"some_struct":{"field":12},"other_struct":{"integer":13}}"""
        ).toDS)

        val expectedSchema = StructType(Seq(
            Field("my_field", LongType, true),
            Field("integer", LongType, true)
        ))

        val mapping = project.mappings("t1").instantiate(session.context)
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("t0") -> df))("main")
        result.schema should be (expectedSchema.sparkType)

        val schema = mapping.describe(Map(MappingOutputIdentifier("t0") -> StructType.of(df.schema)))
        schema("main") should be (expectedSchema)
    }
}
