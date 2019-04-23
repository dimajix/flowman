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

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession


class AssemblerTest extends FlatSpec with Matchers with LocalSparkSession {
    private val inputJson =
        """
          |{
          |  "stupidName": {
          |    "secret": {
          |      "field":123,
          |      "other_field":456
          |    }
          |  },
          |  "lala": {
          |  },
          |  "embedded" : {
          |    "structure": {
          |      "secret": {
          |        "value":"top_secret"
          |      },
          |      "public": "common_knowledge"
          |    },
          |    "old_structure": {
          |      "value": [123, 456]
          |    }
          |  }
          |}""".stripMargin

    "The assembler" should "work" in {
        val spark = this.spark
        import spark.implicits._

        val asm = Assembler.builder()
            .nest("clever_name")(
                _.path("stupidName")
                    .drop("secret.field")
            )
            .columns(
                _.path("")
                    .keep(Seq("lala", "lolo"))
            )
            .columns(
                _.path("")
                    .drop("stupidName")
                    .drop("embedded.structure.secret")
                    .drop("embedded.old_structure")
            )
            .assemble("sub_structure")(
                _.columns(
                    _.path("embedded.old_structure")
                )
            )
            .build()

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        val inputDf = spark.read.json(inputDs)

        val outputDf = asm.reassemble(inputDf)

        val expectedSchema = StructType(Seq(
            StructField("clever_name", StructType(Seq(
                StructField("secret", StructType(Seq(
                    StructField("other_field", LongType)
                )), false)
            )), false),
            StructField("embedded", StructType(Seq(
                StructField("structure", StructType(Seq(
                    StructField("public", StringType)
                )), false)
            )), false),
            StructField("sub_structure", StructType(Seq(
                StructField("value", ArrayType(LongType))
            )), false)
        ))

        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)
    }

    it should "support keep" in {
        val spark = this.spark
        import spark.implicits._

        val asm = Assembler.builder()
            .columns(
                _.path("")
                    .keep("embedded")
                    .drop("embedded.structure.secret")
                    .drop("embedded.old_structure")
            )
            .build()

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        val inputDf = spark.read.json(inputDs)

        val outputDf = asm.reassemble(inputDf)

        val expectedSchema = StructType(Seq(
            StructField("embedded", StructType(Seq(
                StructField("structure", StructType(Seq(
                    StructField("public", StringType)
                )), false)
            )), false)
        ))

        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)
    }

    it should "support nest" in {
        val spark = this.spark
        import spark.implicits._

        val asm = Assembler.builder()
            .nest("clever_name")(
                _.path("stupidName")
                    .drop("secret.field")
            )
            .build()

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        val inputDf = spark.read.json(inputDs)

        val outputDf = asm.reassemble(inputDf)

        val expectedSchema = StructType(Seq(
            StructField("clever_name", StructType(Seq(
                StructField("secret", StructType(Seq(
                    StructField("other_field", LongType)
                )), false)
            )), false)
        ))

        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)
    }

    it should "support lift" in {
        val spark = this.spark
        import spark.implicits._

        val asm = Assembler.builder()
            .lift(
                _.path("stupidName")
                    .column("secret.field")
            )
            .build()

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        val inputDf = spark.read.json(inputDs)

        val outputDf = asm.reassemble(inputDf)

        val expectedSchema = StructType(Seq(
            StructField("field", LongType)
        ))

        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)
    }

    it should "support assembling sub structures" in {
        val spark = this.spark
        import spark.implicits._

        val asm = Assembler.builder()
            .assemble("sub_structure")(
                _.columns(
                    _.path("embedded.old_structure")
                )
            )
            .build()

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        val inputDf = spark.read.json(inputDs)

        val outputDf = asm.reassemble(inputDf)

        val expectedSchema = StructType(Seq(
            StructField("sub_structure", StructType(Seq(
                StructField("value", ArrayType(LongType))
            )), false)
        ))

        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)
    }

//    it should "support renaming a column via assemble" in {
//        val spark = this.spark
//        import spark.implicits._
//
//        val asm = Assembler.builder()
//            .assemble("new_name")(
//                _.columns(
//                    _.path("embedded.old_structure.value")
//                )
//            )
//            .build()
//
//        val inputRecords = Seq(inputJson.replace("\n",""))
//        val inputDs = spark.createDataset(inputRecords)
//        val inputDf = spark.read.json(inputDs)
//
//        val outputDf = asm.reassemble(inputDf)
//
//        val expectedSchema = StructType(Seq(
//            StructField("value", ArrayType(LongType), false)
//        ))
//
//        outputDf.count should be (1)
//        outputDf.schema should be (expectedSchema)
//    }

    it should "support renaming a column via nest" in {
        val spark = this.spark
        import spark.implicits._

        val asm = Assembler.builder()
            .nest("new_name")(
                _.path("embedded.old_structure.value")
            )
            .build()

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        val inputDf = spark.read.json(inputDs)

        val outputDf = asm.reassemble(inputDf)

        val expectedSchema = StructType(Seq(
            StructField("new_name", ArrayType(LongType), true)
        ))

        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)
    }
}
