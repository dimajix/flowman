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

package com.dimajix.flowman.spec.mapping

import org.apache.spark.sql.Row
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
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession


class SchemaMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The SchemaMapping" should "be parsable with columns" in {
        val spec =
            """
              |mappings:
              |  t1:
              |    kind: schema
              |    input: t0
              |    columns:
              |      _2: string
              |      _1: string
              |      _5: string
              |      _3: string
              |      _4: string
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        project.mappings.size should be (1)
        project.mappings.contains("t1") should be (true)

        val mapping = context.getMapping(MappingIdentifier("t1")).asInstanceOf[SchemaMapping]
        mapping.inputs should be (Set(MappingOutputIdentifier("t0")))
        mapping.output should be (MappingOutputIdentifier("project/t1:main"))
        mapping.identifier should be (MappingIdentifier("project/t1"))
        mapping.schema should be (None)
        mapping.columns should be (Seq(
            Field("_2", ftypes.StringType),
            Field("_1", ftypes.StringType),
            Field("_5", ftypes.StringType),
            Field("_3", ftypes.StringType),
            Field("_4", ftypes.StringType)
        ))
    }

    it should "work" in {
        val inputDf = spark.createDataFrame(Seq(
            ("col1", 12),
            ("col2", 23)
        ))
        val inputSchema = com.dimajix.flowman.types.StructType.of(inputDf.schema)

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val mapping = SchemaMapping(
            Mapping.Properties(session.context, name = "map"),
            MappingOutputIdentifier("myview"),
            Seq(Field("_2", FieldType.of("int")))
        )

        mapping.input should be (MappingOutputIdentifier("myview"))
        mapping.columns should be (Seq(Field("_2", FieldType.of("int"))))
        mapping.inputs should be (Set(MappingOutputIdentifier("myview")))
        mapping.output should be (MappingOutputIdentifier("map:main"))
        mapping.identifier should be (MappingIdentifier("map"))

        mapping.describe(executor, Map(MappingOutputIdentifier("myview") -> inputSchema)) should be (Map(
            "main" -> com.dimajix.flowman.types.StructType(Seq(Field("_2", FieldType.of("int"))))
        ))
        mapping.describe(executor, Map(MappingOutputIdentifier("myview") -> inputSchema), "main") should be (
            com.dimajix.flowman.types.StructType(Seq(Field("_2", FieldType.of("int"))))
        )

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("myview") -> inputDf))("main")
            .orderBy("_2")
        result.collect() should be (Seq(
            Row(12),
            Row(23)
        ))
    }

    it should "add NULL columns for missing columns" in {
        val df = spark.createDataFrame(Seq(
            ("col1", 12),
            ("col2", 23)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val mapping = SchemaMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("myview"),
            Seq(
                Field("_2", FieldType.of("int")),
                Field("new", FieldType.of("string"))
            )
        )

        mapping.input should be (MappingOutputIdentifier("myview"))
        mapping.inputs should be (Set(MappingOutputIdentifier("myview")))
        mapping.outputs should be (Set("main"))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("myview") -> df))("main")
            .orderBy("_2")
        result.schema should be (StructType(Seq(
            StructField("_2", IntegerType, false),
            StructField("new", StringType, true)
        )))

        val rows = result.collect()
        rows should be (Seq(
            Row(12, null),
            Row(23, null)
        ))
    }
}
