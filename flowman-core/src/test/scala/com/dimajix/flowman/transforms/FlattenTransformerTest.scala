/*
 * Copyright (C) 2019 The Flowman Authors
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

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession


class FlattenTransformerTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The FlattenTransformer" should "work with nested schemas" in {
        val inputJson =
            """
              |{
              |  "stupid_name": {
              |    "secret_struct": {
              |      "secret_field":123,
              |      "other_field":456
              |    }
              |  }
              |}""".stripMargin

        val spark = this.spark
        import spark.implicits._

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        val inputDf = spark.read.json(inputDs)
        val inputSchema = ftypes.StructType.of(inputDf.schema)

        val xfs = FlattenTransformer(CaseFormat.SNAKE_CASE)

        val expectedSchema = StructType(Seq(
            StructField("stupid_name_secret_struct_other_field", LongType),
            StructField("stupid_name_secret_struct_secret_field", LongType)
        ))

        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (expectedSchema)

        val outputSchema = xfs.transform(inputSchema)
        outputSchema.sparkType should be (expectedSchema)
    }

    it should "flatten arrays" in {
        val inputJson =
            """
              |{
              |  "embedded_stuff" : {
              |    "struct_array": [
              |       {
              |         "key": "k1",
              |         "value": 456
              |       }
              |    ]
              |  }
              |}""".stripMargin

        val spark = this.spark
        import spark.implicits._

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        val inputDf = spark.read.json(inputDs)
        val inputSchema = ftypes.StructType.of(inputDf.schema)

        val xfs = FlattenTransformer(CaseFormat.SNAKE_CASE)

        val expectedSchema = StructType(Seq(
            StructField("embedded_stuff_struct_array", ArrayType(
                StructType(
                    Seq(
                        StructField("key", StringType),
                        StructField("value", LongType)
                    )
                )
            ))
        ))

        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (expectedSchema)

        val outputSchema = xfs.transform(inputSchema)
        outputSchema.sparkType should be (expectedSchema)
    }

}
