/*
 * Copyright 2021 Kaya Kupferschmidt
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
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema.EmbeddedSchema
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class NullMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The NullMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  empty1:
              |    kind: null
              |    columns:
              |      str_col: string
              |      int_col: integer
              |
              |  empty2:
              |    kind: empty
              |    schema:
              |      kind: embedded
              |      fields:
              |         - name: str_col
              |           type: string
              |         - name: int_col
              |           type: integer
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val mapping1 = context.getMapping(MappingIdentifier("empty1")).asInstanceOf[NullMapping]
        mapping1 shouldBe a[NullMapping]

        mapping1.category should be ("mapping")
        mapping1.kind should be ("null")
        mapping1.columns should be (Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))
        mapping1.schema should be (None)
        mapping1.output should be (MappingOutputIdentifier("project/empty1:main"))
        mapping1.outputs should be (Seq("main"))
    }

    it should "create empty DataFrames with specified columns" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val executor = session.execution

        val mapping = NullMapping(
            Mapping.Properties(context, "empty"),
            Seq(
                Field("str_col", StringType),
                Field("int_col", IntegerType)
            ),
            None
        )

        mapping.category should be ("mapping")
        //mapping.kind should be ("null")
        mapping.outputs should be (Seq("main"))
        mapping.output should be (MappingOutputIdentifier("empty"))

        mapping.describe(executor, Map()) should be (Map(
            "main" -> new StructType(Seq(
                Field("str_col", StringType),
                Field("int_col", IntegerType)
            ))
        ))
        mapping.describe(executor, Map(), "main") should be (
            new StructType(Seq(
                Field("str_col", StringType),
                Field("int_col", IntegerType)
            ))
        )

        val df = executor.instantiate(mapping, "main")
        df.columns should contain("str_col")
        df.columns should contain("int_col")
        df.count() should be (0)
    }

    it should "create empty DataFrames with specified schema" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val executor = session.execution

        val mapping = NullMapping(
            Mapping.Properties(context, "empty"),
            Seq(),
            Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType)
                )
            ))
        )

        mapping.category should be ("mapping")
        //mapping.kind should be ("null")
        mapping.outputs should be (Seq("main"))
        mapping.output should be (MappingOutputIdentifier("empty"))

        mapping.describe(executor, Map()) should be (Map(
            "main" -> new StructType(Seq(
                Field("str_col", StringType),
                Field("int_col", IntegerType)
            ))
        ))
        mapping.describe(executor, Map(), "main") should be (
            new StructType(Seq(
                Field("str_col", StringType),
                Field("int_col", IntegerType)
            ))
        )

        val df = executor.instantiate(mapping, "main")
        df.columns should contain("str_col")
        df.columns should contain("int_col")
        df.count() should be (0)
    }

    it should "raise an error on wrong construction" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context

        an[IllegalArgumentException] should be thrownBy (NullMapping(
            Mapping.Properties(context, "empty"),
            Seq(),
            None
        ))

        an[IllegalArgumentException] should be thrownBy (NullMapping(
            Mapping.Properties(context, "empty"),
            Seq(
                Field("str_col", StringType),
                Field("int_col", IntegerType)
            ),
            Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", StringType),
                    Field("int_col", IntegerType)
                )
            ))
        ))
    }
}
