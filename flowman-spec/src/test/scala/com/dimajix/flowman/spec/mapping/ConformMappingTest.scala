/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.transforms.CaseFormat
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession


class ConformMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    private val inputJson =
        """
          |{
          |  "str_col": "1234",
          |  "double_col": 12.34,
          |  "embedded" : {
          |    "some_string": "567",
          |    "struct_array": [
          |       {
          |         "value": 123
          |       },
          |       {
          |         "value": 456
          |       }
          |    ]
          |  }
          |}""".stripMargin

    private var inputDf: DataFrame = _

    override def beforeAll(): Unit = {
        super.beforeAll()

        val spark = this.spark
        import spark.implicits._

        val inputRecords = Seq(inputJson.replace("\n", ""))
        val inputDs = spark.createDataset(inputRecords)
        inputDf = spark.read.json(inputDs)
    }

    "A ConformMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  my_structure:
              |    kind: conform
              |    input: some_mapping
              |    naming: camelCase
              |    types:
              |      long: string
              |      date: timestamp
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val mapping = project.mappings("my_structure")

        mapping shouldBe an[ConformMappingSpec]

        val session = Session.builder().build()
        val context = session.getContext(project)
        val instance = context.getMapping(MappingIdentifier("my_structure"))
        instance shouldBe an[ConformMapping]
    }

    it should "support changing types in DataFrames" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = ConformMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Map(
                "string" -> ftypes.IntegerType
            )
        )

        val expectedSchema = StructType(Seq(
            StructField("double_col", DoubleType),
            StructField("embedded", StructType(Seq(
                StructField("some_string", IntegerType),
                StructField("struct_array", ArrayType(
                    StructType(Seq(
                        StructField("value", LongType)
                    ))
                ))
            ))),
            StructField("str_col", IntegerType)
        ))

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")
        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)), "main")
        outputSchema.sparkType should be (expectedSchema)
    }

    it should "throw an error for arrays" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = ConformMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Map(
                "long" -> ftypes.IntegerType
            )
        )

        an[UnsupportedOperationException] shouldBe thrownBy(mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf)))
    }

    it should "support renaming fields" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = ConformMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            naming=Some(CaseFormat.CAMEL_CASE)
        )

        val expectedSchema = StructType(Seq(
            StructField("doubleCol", DoubleType),
            StructField("embedded", StructType(Seq(
                StructField("someString", StringType),
                StructField("structArray", ArrayType(
                    StructType(Seq(
                        StructField("value", LongType)
                    ))
                ))
            ))),
            StructField("strCol", StringType)
        ))

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")
        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)), "main")
        outputSchema.sparkType should be (expectedSchema)
    }

    it should "support flattening nested structures" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = ConformMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            naming=Some(CaseFormat.SNAKE_CASE),
            flatten=true
        )

        val expectedSchema = StructType(Seq(
            StructField("double_col", DoubleType),
            StructField("embedded_some_string", StringType),
            StructField("embedded_struct_array", ArrayType(
                StructType(Seq(
                    StructField("value", LongType)
                ))
            )),
            StructField("str_col", StringType)
        ))

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")
        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)))("main")
        outputSchema.sparkType should be (expectedSchema)
    }
}
