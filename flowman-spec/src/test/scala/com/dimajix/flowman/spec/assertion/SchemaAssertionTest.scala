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

package com.dimajix.flowman.spec.assertion

import java.time.Instant

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.AssertionTestResult
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.schema.EmbeddedSchema
import com.dimajix.flowman.types.ArrayType
import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.MapType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.sql.DataFrameBuilder
import com.dimajix.spark.sql.DataFrameUtils
import com.dimajix.spark.testing.LocalSparkSession


class SchemaAssertionTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The SchemaAssertion" should "be parseable with columns" in {
        val spec =
            """
              |kind: schema
              |mapping: some_mapping
              |columns:
              |  col_1: string
              |  col_2: int
              |""".stripMargin

        val assertionSpec = ObjectMapper.parse[AssertionSpec](spec)
        assertionSpec shouldBe a[SchemaAssertionSpec]

        val context = RootContext.builder().build()
        val assertion = assertionSpec.instantiate(context).asInstanceOf[SchemaAssertion]
        assertion.name should be ("")
        assertion.mapping should be (MappingOutputIdentifier("some_mapping"))
        assertion.columns should be (Seq(Field("col_1", StringType), Field("col_2", IntegerType)))
        assertion.schema should be (None)
    }

    it should "be parseable with embedded schema" in {
        val spec =
            """
              |kind: schema
              |mapping: some_mapping
              |schema:
              |  kind: inline
              |  fields:
              |    - name: col_1
              |      type: string
              |    - name: col_2
              |      type: int
              |""".stripMargin

        val assertionSpec = ObjectMapper.parse[AssertionSpec](spec)
        assertionSpec shouldBe a[SchemaAssertionSpec]

        val context = RootContext.builder().build()
        val assertion = assertionSpec.instantiate(context).asInstanceOf[SchemaAssertion]
        assertion.name should be ("")
        assertion.mapping should be (MappingOutputIdentifier("some_mapping"))
        assertion.columns should be (Seq())
        assertion.schema should be (Some(EmbeddedSchema(
            Schema.Properties(context, name="embedded", kind="inline"),
            fields = Seq(Field("col_1", StringType), Field("col_2", IntegerType))
        )))
    }

    it should "compare schema with ordering" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val assertion = SchemaAssertion(
            Assertion.Properties(context),
            MappingOutputIdentifier("df"),
            columns = Seq(
                Field("col_1", StringType),
                Field("col_2", IntegerType),
                Field("map", MapType(StringType, DoubleType)),
                Field("array", ArrayType(BooleanType)),
                Field("struct", StructType(Seq(
                    Field("nested_col_1", StringType),
                    Field("nested_col_2", IntegerType)
                )))
            )
        )

        val df1 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("col_2", IntegerType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df1)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, true, Instant.now()))
            ).withoutTime
        )

        val df2 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("col_2", IntegerType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_2", IntegerType),
                Field("nested_col_1", StringType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df2)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )

        val df3 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_2", IntegerType),
            Field("col_1", StringType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df3)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )

        val df4 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df4)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )

        val df5 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("col_2", IntegerType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df5)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )

        val df6 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("col_2", IntegerType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", IntegerType)
            ))),
            Field("extra_col", IntegerType)
        )).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df6)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )
    }

    it should "compare schema ignoring ordering" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val assertion = SchemaAssertion(
            Assertion.Properties(context),
            MappingOutputIdentifier("df"),
            columns = Seq(
                Field("col_1", StringType),
                Field("col_2", IntegerType),
                Field("map", MapType(StringType, DoubleType)),
                Field("array", ArrayType(BooleanType)),
                Field("struct", StructType(Seq(
                    Field("nested_col_1", StringType),
                    Field("nested_col_2", IntegerType)
                )))
            ),
            ignoreOrder = true
        )

        val df1 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("col_2", IntegerType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df1)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, true))
            ).withoutTime
        )

        val df2 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("col_2", IntegerType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_2", IntegerType),
                Field("nested_col_1", StringType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df2)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, true))
            ).withoutTime
        )

        val df3 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_2", IntegerType),
            Field("col_1", StringType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df3)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, true))
            ).withoutTime
        )

        val df4 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df4)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )

        val df5 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("col_2", IntegerType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df5)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )

        val df6 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("col_2", IntegerType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", IntegerType)
            ))),
            Field("extra_col", IntegerType)
        )).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df6)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )

    }

    it should "compare schema ignoring case" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val assertion = SchemaAssertion(
            Assertion.Properties(context),
            MappingOutputIdentifier("df"),
            columns = Seq(
                Field("col_1", StringType),
                Field("struct", StructType(Seq(
                    Field("nested_col_1", StringType),
                    Field("nested_col_2", IntegerType)
                )))
            ),
            ignoreCase = true
        )

        val df1 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("COL_1", StringType),
            Field("struct", StructType(Seq(
                Field("NESTED_COL_1", StringType),
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df1)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, true))
            ).withoutTime
        )

        val df2 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("COL_1", StringType),
            Field("struct", StructType(Seq(
                Field("NESTED_COL_1", IntegerType),
                Field("nested_col_1", StringType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df2)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )

        val df4 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("COL_1", StringType),
            Field("struct", StructType(Seq(
                Field("nested_col_2", StringType),
                Field("NESTED_COL_1", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df4)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )
    }

    it should "compare schema ignoring nullability" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val assertion = SchemaAssertion(
            Assertion.Properties(context),
            MappingOutputIdentifier("df"),
            columns = Seq(
                Field("col_1", StringType, nullable = false),
                Field("struct", StructType(Seq(
                    Field("nested_col_1", StringType, nullable = false),
                    Field("nested_col_2", IntegerType)
                )))
            ),
            ignoreNullability = false
        )

        val df1 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType, nullable = false),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType, nullable = false),
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df1)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, true))
            ).withoutTime
        )

        val df2 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType, nullable = false),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", IntegerType, nullable = false)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df2)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )

        val df3 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType, nullable = false),
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df3)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )
    }

    it should "compare schema with nullability" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val assertion = SchemaAssertion(
            Assertion.Properties(context),
            MappingOutputIdentifier("df"),
            columns = Seq(
                Field("col_1", StringType, nullable = false),
                Field("struct", StructType(Seq(
                    Field("nested_col_1", StringType, nullable = false),
                    Field("nested_col_2", IntegerType)
                )))
            ),
            ignoreNullability = true
        )

        val df1 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType, nullable = false),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType, nullable = false),
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df1)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, true))
            ).withoutTime
        )

        val df2 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType, nullable = false),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", IntegerType, nullable = false)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df2)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, true))
            ).withoutTime
        )

        val df3 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType, nullable = false),
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df3)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, true))
            ).withoutTime
        )
    }

    it should "compare schema ignoring types" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val assertion = SchemaAssertion(
            Assertion.Properties(context),
            MappingOutputIdentifier("df"),
            columns = Seq(
                Field("col_1", StringType),
                Field("col_2", IntegerType),
                Field("map", MapType(StringType, DoubleType)),
                Field("array", ArrayType(BooleanType)),
                Field("struct", StructType(Seq(
                    Field("nested_col_1", StringType),
                    Field("nested_col_2", IntegerType)
                )))
            ),
            ignoreTypes = true
        )

        val df1 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", DoubleType),
            Field("col_2", IntegerType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_1", FloatType),
                Field("nested_col_2", StringType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df1)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, true))
            ).withoutTime
        )

        val df2 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("col_2", IntegerType),
            Field("map", StringType),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df2)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )

        val df3 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("col_2", IntegerType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", StringType),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", IntegerType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df3)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )

        val df4 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("col_2", IntegerType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StringType))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df4)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )

        val df5 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("col_2", IntegerType),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", DoubleType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df5)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, true))
            ).withoutTime
        )

        val df6 = DataFrameBuilder.ofSchema(execution.spark, StructType(Seq(
            Field("col_1", StringType),
            Field("col_2", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", DoubleType)
            ))),
            Field("map", MapType(StringType, DoubleType)),
            Field("array", ArrayType(BooleanType)),
            Field("struct", StructType(Seq(
                Field("nested_col_1", StringType),
                Field("nested_col_2", DoubleType)
            ))))).sparkType)
        assertion.execute(execution, Map(MappingOutputIdentifier("df") -> df6)).withoutTime should be (
            AssertionResult(
                assertion,
                Seq(AssertionTestResult("schema for 'df:main'", None, false))
            ).withoutTime
        )
    }
}
