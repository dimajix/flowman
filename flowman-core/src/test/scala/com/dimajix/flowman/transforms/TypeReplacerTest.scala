/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman.transforms

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.VarcharType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.sql.SchemaUtils
import com.dimajix.spark.testing.LocalSparkSession


class TypeReplacerTest extends AnyFlatSpec with Matchers with LocalSparkSession {
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

    "A TypeReplacer" should "transform DataFrames correctly" in {
        val mapping = TypeReplacer(
                Map(
                    "string" -> ftypes.IntegerType
                )
            )

        val outputDf = mapping.transform(inputDf)

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

        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)
    }

    it should "provide a correct output schema" in {
        val mapping = TypeReplacer(
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

        val outputSchema = mapping.transform(ftypes.StructType.of(inputDf.schema))
        outputSchema.sparkType should be (expectedSchema)
        outputSchema.catalogType should be (expectedSchema)
    }

    it should "support simple types" in {
        val rawInputDf = spark.createDataFrame(Seq(
            ("1","1"),
            ("123","123")
        ))


        val simpleTypes = Seq(
            "string", "text",
            "bool", "boolean",
            "byte", "tinyint",
            "short", "smallint",
            "int", "integer",
            "long", "bigint",
            "float",
            "double"
        )
        for (typeName <- simpleTypes) {
            val fieldType =  ftypes.FieldType.of(typeName)
            val inputSchema = ftypes.StructType(Seq(
                ftypes.Field("_1", fieldType)
            ))
            val xfs = SchemaEnforcer(inputSchema.catalogType)
            val inputDf = xfs.transform(rawInputDf)

            val mapping = TypeReplacer(
                Map(
                    typeName -> ftypes.FloatType
                )
            )
            val expectedSchema = StructType(Seq(
                StructField("_1", FloatType)
            ))

            val outputSchema = mapping.transform(ftypes.StructType.of(inputDf.schema))
            outputSchema.sparkType should be(expectedSchema)
            outputSchema.catalogType should be(expectedSchema)

            val outputDf = mapping.transform(inputDf)
            outputDf.schema should be(expectedSchema)
        }
    }

    it should "support extended string types (1)" in {
        val mapping = TypeReplacer(
            Map(
                "string" -> ftypes.IntegerType
            )
        )

        val rawInputDf = spark.createDataFrame(Seq(
            ("col", 1.0f),
            ("col123", 2345.0f)
        ))
        val inputSchema = ftypes.StructType(Seq(
            ftypes.Field("_1", ftypes.VarcharType(10)),
            ftypes.Field("_2", ftypes.FloatType)
        ))
        val xfs = SchemaEnforcer(inputSchema.catalogType)
        val inputDf = xfs.transform(rawInputDf)
        val expectedSchema = StructType(Seq(
            StructField("_1", IntegerType),
            StructField("_2", FloatType, false)
        ))

        val outputSchema = mapping.transform(ftypes.StructType.of(inputDf.schema))
        outputSchema.sparkType should be(expectedSchema)
        outputSchema.catalogType should be(expectedSchema)

        val outputDf = mapping.transform(inputDf)
        outputDf.schema should be (expectedSchema)
    }

    it should "support extended string types (2)" in {
        val mapping = TypeReplacer(
            Map(
                "int" -> ftypes.FloatType
            )
        )

        val rawInputDf = spark.createDataFrame(Seq(
            ("col", 1),
            ("col123", 2345)
        ))
        val inputSchema = ftypes.StructType(Seq(
            ftypes.Field("_1", ftypes.VarcharType(10)),
            ftypes.Field("_2", ftypes.IntegerType)
        ))
        val xfs = SchemaEnforcer(inputSchema.catalogType)
        val inputDf = xfs.transform(rawInputDf)
        val expectedSchema = StructType(Seq(
            StructField("_1", VarcharType(10)),
            StructField("_2", FloatType, false)
        ))
        val expectedSimpleSchema = StructType(Seq(
            StructField("_1", StringType),
            StructField("_2", FloatType, false)
        ))

        val outputSchema = mapping.transform(ftypes.StructType.of(inputDf.schema))
        SchemaUtils.dropExtendedTypeInfo(outputSchema.sparkType) should be(expectedSimpleSchema)
        outputSchema.catalogType should be(expectedSchema)

        val outputDf = mapping.transform(inputDf)
        SchemaUtils.dropExtendedTypeInfo(outputDf.schema) should be(expectedSimpleSchema)
        SchemaUtils.recoverCharVarchar(outputDf.schema) should be(expectedSchema)
    }

    it should "throw an error for arrays" in {
        val mapping = TypeReplacer(
            Map(
                "long" -> ftypes.IntegerType
            )
        )

        an[UnsupportedOperationException] shouldBe thrownBy(mapping.transform(inputDf))
    }
}
