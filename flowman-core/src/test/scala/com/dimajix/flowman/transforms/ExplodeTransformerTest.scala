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

package com.dimajix.flowman.transforms

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.testing.LocalSparkSession
import com.dimajix.flowman.transforms.schema.Path
import com.dimajix.flowman.{types => ftypes}


class ExplodeTransformerTest extends FlatSpec with Matchers with LocalSparkSession {
    private var inputDf: DataFrame = _
    private var inputSchema: ftypes.StructType = _

    override def beforeAll(): Unit = {
        super.beforeAll()

        val spark = this.spark
        import spark.implicits._

        val inputJson =
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
              |    ]
              |  }
              |}""".stripMargin

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        inputDf = spark.read.json(inputDs)
        inputSchema = ftypes.StructType.of(inputDf.schema)
    }

    "The ExplodeTransformer" should "work" in {
        val xfs = ExplodeTransformer(
            Path("some_struct.some_array"),
            Seq(),
            Seq(),
            Map()
        )
        val resultDf = xfs.transform(inputDf)
        resultDf.schema should be(StructType(Seq(
            StructField("outer_col0", StringType),
            StructField("outer_col1", StringType),
            StructField("some_struct", StructType(Seq(
                StructField("some_field", StringType)
            ))),
            StructField("some_array", StructType(Seq(
                StructField("inner_col0", LongType),
                StructField("inner_col1", LongType)
            )))
        )))

        val resultSchema = xfs.transform(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "support dropping fields" in {
        val xfs = ExplodeTransformer(
            Path("some_struct.some_array"),
            Seq(),
            Seq(Path("some_struct")),
            Map()
        )
        val resultDf = xfs.transform(inputDf)
        resultDf.schema should be(StructType(Seq(
            StructField("outer_col0", StringType),
            StructField("outer_col1", StringType),
            StructField("some_array", StructType(Seq(
                StructField("inner_col0", LongType),
                StructField("inner_col1", LongType)
            )))
        )))

        val resultSchema = xfs.transform(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "support keeping fields" in {
        val xfs = ExplodeTransformer(
            Path("some_struct.some_array"),
            Seq(Path("outer_col0")),
            Seq(),
            Map()
        )
        val resultDf = xfs.transform(inputDf)
        resultDf.schema should be(StructType(Seq(
            StructField("outer_col0", StringType),
            StructField("some_array", StructType(Seq(
                StructField("inner_col0", LongType),
                StructField("inner_col1", LongType)
            )))
        )))

        val resultSchema = xfs.transform(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "support dropping all and renaming" in {
        val xfs = ExplodeTransformer(
            Path("some_struct.some_array"),
            Seq(),
            Seq(Path("*")),
            Map("result" -> Path("outer_col0"))
        )
        val resultDf = xfs.transform(inputDf)
        resultDf.schema should be(StructType(Seq(
            StructField("result", StringType),
            StructField("some_array", StructType(Seq(
                StructField("inner_col0", LongType),
                StructField("inner_col1", LongType)
            )))
        )))

        val resultSchema = xfs.transform(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "support dropping all and keeping" in {
        val xfs = ExplodeTransformer(
            Path("some_struct.some_array"),
            Seq(Path("some_struct.some_field")),
            Seq(Path("*")),
            Map()
        )
        val resultDf = xfs.transform(inputDf)
        resultDf.schema should be(StructType(Seq(
            StructField("some_array", StructType(Seq(
                StructField("inner_col0", LongType),
                StructField("inner_col1", LongType)
            )))
        )))

        val resultSchema = xfs.transform(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }
}
