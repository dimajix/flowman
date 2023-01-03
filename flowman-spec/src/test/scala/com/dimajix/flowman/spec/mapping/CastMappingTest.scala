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

package com.dimajix.flowman.spec.mapping

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.types.DecimalType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.VarcharType
import com.dimajix.spark.sql.DataFrameBuilder
import com.dimajix.spark.sql.SchemaUtils
import com.dimajix.spark.testing.LocalSparkSession


class CastMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "An CastMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  m1:
              |    kind: cast
              |    input: some_mapping
              |    columns:
              |      id: VARCHAR(10)
              |      amount: DECIMAL(16,3)
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)

        val mapping = project.mappings("m1")
        mapping shouldBe a[CastMappingSpec]

        val instance = context.getMapping(MappingIdentifier("m1"))
        instance shouldBe a[CastMapping]

        val typedInstance = instance.asInstanceOf[CastMapping]
        typedInstance.input should be (MappingOutputIdentifier("some_mapping"))
        typedInstance.inputs should be (Set(MappingOutputIdentifier("some_mapping")))
        typedInstance.outputs should be (Set("main"))
        typedInstance.columns should be (Map("id" -> VarcharType(10), "amount" -> DecimalType(16,3)))

        session.shutdown()
    }

    it should "work" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution

        val inputSchema = StructType(Seq(
            Field("id", StringType),
            Field("name", StringType),
            Field("amount", DoubleType)
        ))

        val mapping = CastMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input"),
            Map("id" -> VarcharType(10), "amount" -> DecimalType(16,3))
        )

        val outputSchema = StructType(Seq(
            Field("id", VarcharType(10)),
            Field("name", StringType),
            Field("amount", DecimalType(16,3))
        ))
        mapping.describe(execution, Map(MappingOutputIdentifier("input") -> inputSchema)) should be (Map("main" -> outputSchema))

        val input = DataFrameBuilder.ofSchema(execution.spark, inputSchema.sparkType)
        val result = mapping.execute(execution, Map(MappingOutputIdentifier("input") -> input))("main")
        SchemaUtils.normalize(result.schema) should be (SchemaUtils.normalize(outputSchema.sparkType))

        session.shutdown()
    }
}
