/*
 * Copyright 2019 Kaya Kupferschmidt
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
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.transforms.CaseFormat
import com.dimajix.flowman.transforms.schema.Path
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession


class ExplodeMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    private val inputJson =
        """
          |{
          |  "outer_col0": "oc0",
          |  "outer_col1": "oc1",
          |  "some_struct": {
          |    "some_field": "lala",
          |    "some_array": [ {
          |        "inner_col0":123,
          |        "inner_col1":456
          |      }
          |    ],
          |    "simple_array" : [1,2]
          |  }
          |}""".stripMargin
    private var inputDf:DataFrame = _

    override def beforeAll() : Unit = {
        super.beforeAll()

        val spark = this.spark
        import spark.implicits._

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        inputDf = spark.read.json(inputDs)
    }

    "An explode mapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  updates:
              |    kind: explode
              |    input: events
              |    array: data.nestedArray
              |    flatten: true
              |    outerColumns:
              |      # Drop everything from parent entity, except...
              |      drop: "*"
              |      # ...keeping the data operation and the parent id (as a foreign key)
              |      rename:
              |        data_operation: dataOperation
              |        contract_id: data.id
              |    innerColumns:
              |      # Remove all remaining nested arrays
              |      drop:
              |        - someList
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val mapping = project.mappings("updates")

        mapping shouldBe an[ExplodeMappingSpec]
    }

    it should "work" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = ExplodeMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Path("some_struct.some_array"),
            ExplodeMapping.Columns(
                Seq(),
                Seq(),
                Map()
            ),
            ExplodeMapping.Columns(
                Seq(),
                Seq(),
                Map()
            ),
            true,
            CaseFormat.SNAKE_CASE
        )

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")
        val expectedSchema = StructType(Seq(
            StructField("outer_col0", StringType),
            StructField("outer_col1", StringType),
            StructField("some_struct_simple_array", ArrayType(LongType)),
            StructField("some_struct_some_field", StringType),
            StructField("inner_col0", LongType),
            StructField("inner_col1", LongType)
        ))

        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)), "main")
        outputSchema.sparkType should be (expectedSchema)
    }

    it should "select specified columns" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = ExplodeMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Path("some_struct.some_array"),
            ExplodeMapping.Columns(
                Seq(Path("*")),
                Seq(Path("some_struct.some_field")),
                Map()
            ),
            ExplodeMapping.Columns(
                Seq(Path("*")),
                Seq(Path("inner_col0")),
                Map()
            ),
            true,
            CaseFormat.SNAKE_CASE
        )

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")
        val expectedSchema = StructType(Seq(
            StructField("outer_col0", StringType),
            StructField("outer_col1", StringType),
            StructField("some_struct_simple_array", ArrayType(LongType)),
            StructField("inner_col1", LongType)
        ))

        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)), "main")
        outputSchema.sparkType should be (expectedSchema)
    }

    it should "support renaming columns" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = ExplodeMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Path("some_struct.some_array"),
            ExplodeMapping.Columns(
                Seq(Path("*")),
                Seq(Path("some_struct.some_field"), Path("some_struct.simple_array")),
                Map("some_field" -> Path("some_struct.some_field"))
            ),
            ExplodeMapping.Columns(
                Seq(Path("*")),
                Seq(Path("inner_col0")),
                Map("result" -> Path("inner_col0"))
            ),
            true,
            CaseFormat.SNAKE_CASE
        )

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")
        val expectedSchema = StructType(Seq(
            StructField("outer_col0", StringType),
            StructField("outer_col1", StringType),
            StructField("some_field", StringType),
            StructField("inner_col1", LongType),
            StructField("result", LongType)
        ))

        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)), "main")
        outputSchema.sparkType should be (expectedSchema)
    }

    it should "explode simple arrays" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = ExplodeMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Path("some_struct.simple_array"),
            ExplodeMapping.Columns(
                Seq(Path("outer_col0")),
                Seq(),
                Map()
            ),
            ExplodeMapping.Columns(
                Seq(Path("*")),
                Seq(),
                Map()
            ),
            true,
            CaseFormat.SNAKE_CASE
        )

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")
        val expectedSchema = StructType(Seq(
            StructField("outer_col0", StringType),
            StructField("simple_array", LongType)
        ))

        outputDf.count should be (2)
        outputDf.schema should be (expectedSchema)

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)), "main")
        outputSchema.sparkType should be (expectedSchema)
    }
}
