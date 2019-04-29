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

package com.dimajix.flowman.transforms

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.{types => ftypes}


class TypeReplacerTest extends FlatSpec with Matchers with LocalSparkSession {
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
        val mapping = new TypeReplacer(
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
        val mapping = new TypeReplacer(
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
    }

    it should "throw an error for arrays" in {
        val mapping = new TypeReplacer(
            Map(
                "long" -> ftypes.IntegerType
            )
        )

        an[UnsupportedOperationException] shouldBe thrownBy(mapping.transform(inputDf))
    }
}