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
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.transforms.schema.Path
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession


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
          |  "lala": 23,
          |  "embedded" : {
          |    "structure": {
          |      "secret": {
          |        "value":"top_secret"
          |      },
          |      "public": "common_knowledge"
          |    },
          |    "old_structure": {
          |      "value": [123, 456]
          |    },
          |    "struct_array": [
          |       {
          |         "key": "k2",
          |         "value": 123
          |       },
          |       {
          |         "key": "k1",
          |         "value": 456
          |       }
          |    ]
          |  },
          |  "top_array" : [
          |     {
          |       "key": "k2",
          |       "value": 123
          |     },
          |     {
          |       "key": "k1",
          |       "value": 456
          |     }
          |  ]
          |}""".stripMargin

    private var inputDf:DataFrame = _
    private var inputSchema: ftypes.StructType = _

    override def beforeAll() : Unit = {
        super.beforeAll()

        val spark = this.spark
        import spark.implicits._

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        inputDf = spark.read.json(inputDs)
        inputSchema = ftypes.StructType.of(inputDf.schema)
    }

    "The assembler" should "work" in {
        val asm = Assembler.builder()
            .nest("clever_name")(
                _.path(Path("stupidName"))
                    .drop(Path("secret.field"))
            )
            .columns(
                _.path(Path(""))
                    .keep(Seq(Path("lala")))
            )
            .columns(
                _.path(Path(""))
                    .drop(Path("stupidName"))
                    .drop(Path("embedded.structure.secret"))
                    .drop(Path("embedded.old_structure"))
                    .drop(Path("top_array"))
            )
            .assemble("sub_structure")(
                _.columns(
                    _.path(Path("embedded.old_structure"))
                )
            )
            .build()

        val resultDf = asm.reassemble(inputDf)

        val expectedSchema = StructType(Seq(
            StructField("clever_name", StructType(Seq(
                StructField("secret", StructType(Seq(
                    StructField("other_field", LongType)
                )))
            ))),
            StructField("lala", LongType),
            StructField("embedded", StructType(Seq(
                StructField("struct_array", ArrayType(
                    StructType(Seq(
                        StructField("key", StringType),
                        StructField("value", LongType)
                    ))
                )),
                StructField("structure", StructType(Seq(
                    StructField("public", StringType)
                )))
            ))),
            StructField("lala", LongType),
            StructField("sub_structure", StructType(Seq(
                StructField("value", ArrayType(LongType))
            )))
        ))

        resultDf.count should be (1)
        resultDf.schema should be (expectedSchema)

        val resultSchema = asm.reassemble(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "throw an exception on missing columns in 'keep'" in {
        val asm = Assembler.builder()
            .nest("clever_name")(
                _.path(Path("stupidName"))
                    .drop(Path("secret.field"))
            )
            .columns(
                _.path(Path(""))
                    .keep(Seq(Path("lala"), Path("lolo")))
            )
            .build()

        an[AnalysisException] shouldBe thrownBy(asm.reassemble(inputDf))
    }

    it should "support keep" in {
        val asm = Assembler.builder()
            .columns(
                _.path(Path(""))
                    .keep(Path("embedded"))
                    .drop(Path("embedded.structure.secret"))
                    .drop(Path("embedded.old_structure"))
            )
            .build()

        val expectedSchema = StructType(Seq(
            StructField("embedded", StructType(Seq(
                StructField("struct_array", ArrayType(
                    StructType(Seq(
                        StructField("key", StringType),
                        StructField("value", LongType)
                    ))
                )),
                StructField("structure", StructType(Seq(
                    StructField("public", StringType)
                )))
            )))
        ))

        val resultDf = asm.reassemble(inputDf)
        resultDf.count should be (1)
        resultDf.schema should be (expectedSchema)

        val resultSchema = asm.reassemble(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "support nest" in {
        val asm = Assembler.builder()
            .nest("clever_name")(
                _.path(Path("stupidName"))
                    .drop(Path("secret.field"))
            )
            .build()

        val expectedSchema = StructType(Seq(
            StructField("clever_name", StructType(Seq(
                StructField("secret", StructType(Seq(
                    StructField("other_field", LongType)
                )))
            )))
        ))

        val resultDf = asm.reassemble(inputDf)
        resultDf.count should be (1)
        resultDf.schema should be (expectedSchema)

        val resultSchema = asm.reassemble(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "support lift" in {
        val asm = Assembler.builder()
            .lift(
                _.path(Path("stupidName"))
                    .column(Path("secret.field"))
            )
            .build()

        val expectedSchema = StructType(Seq(
            StructField("field", LongType)
        ))

        val resultDf = asm.reassemble(inputDf)
        resultDf.count should be (1)
        resultDf.schema should be (expectedSchema)

        val resultSchema = asm.reassemble(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "support assembling sub structures" in {
        val asm = Assembler.builder()
            .assemble("sub_structure")(
                _.columns(
                    _.path(Path("embedded.old_structure"))
                )
            )
            .build()

        val expectedSchema = StructType(Seq(
            StructField("sub_structure", StructType(Seq(
                StructField("value", ArrayType(LongType))
            )))
        ))

        val resultDf = asm.reassemble(inputDf)
        resultDf.count should be (1)
        resultDf.schema should be (expectedSchema)

        val resultSchema = asm.reassemble(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

//    it should "support renaming a column via assemble" in {
//        val spark = this.spark
//        import spark.implicits._
//
//        val asm = Assembler.builder()
//            .assemble("new_name")(
//                _.columns(
//                    _.path(Path("embedded.old_structure.value"))
//                )
//            )
//            .build()
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
        val asm = Assembler.builder()
            .nest("new_name")(
                _.path(Path("embedded.old_structure.value"))
            )
            .build()

        val expectedSchema = StructType(Seq(
            StructField("new_name", ArrayType(LongType), true)
        ))

        val resultDf = asm.reassemble(inputDf)
        resultDf.count should be (1)
        resultDf.schema should be (expectedSchema)

        val resultSchema = asm.reassemble(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "not ignore non-existing paths and structs" in {
        val asm = Assembler.builder()
            .nest("new_name")(
                _.path(Path("embedded.no_such_field"))
            )
            .build()

        an[IllegalArgumentException] shouldBe thrownBy(asm.reassemble(inputDf))
    }

    it should "support explode on simple arrays" in {
        val asm = Assembler.builder()
            .explode(
                _.path(Path("embedded.old_structure.value"))
            )
            .build()

        val expectedSchema = StructType(Seq(
            StructField("value", LongType)
        ))

        val resultDf = asm.reassemble(inputDf)
        resultDf.count should be (2)
        resultDf.schema should be (expectedSchema)

        val resultSchema = asm.reassemble(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "support top level explode on complex arrays with rename" in {
        val asm = Assembler.builder()
            .explode("array")(
                _.path(Path("top_array"))
            )
            .build()

        val expectedSchema = StructType(Seq(
            StructField("array", StructType(Seq(
                StructField("key", StringType),
                StructField("value", LongType)
            )), true)
        ))

        val resultDf = asm.reassemble(inputDf)
        resultDf.count should be (2)
        resultDf.schema should be (expectedSchema)

        val resultSchema = asm.reassemble(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "support nested explode on complex arrays" in {
        val asm = Assembler.builder()
            .explode(
                _.path(Path("embedded.struct_array"))
            )
            .build()

        val expectedSchema = StructType(Seq(
            StructField("struct_array", StructType(Seq(
                StructField("key", StringType),
                StructField("value", LongType)
            )), true)
        ))

        val resultDf = asm.reassemble(inputDf)
        resultDf.count should be (2)
        resultDf.schema should be (expectedSchema)

        val resultSchema = asm.reassemble(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "support nested explode on complex arrays with rename" in {
        val asm = Assembler.builder()
            .explode("array")(
                _.path(Path("embedded.struct_array"))
            )
            .build()

        val expectedSchema = StructType(Seq(
            StructField("array", StructType(Seq(
                StructField("key", StringType),
                StructField("value", LongType)
            )), true)
        ))

        val resultDf = asm.reassemble(inputDf)
        resultDf.count should be (2)
        resultDf.schema should be (expectedSchema)

        val resultSchema = asm.reassemble(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "throw an error for explode with non-existing paths" in {
        val asm = Assembler.builder()
            .explode("array")(
                _.path(Path("embedded.no_such_path"))
            )
            .build()

        an[IllegalArgumentException] shouldBe thrownBy(asm.reassemble(inputDf))
    }

    it should "support renaming" in {
        val asm = Assembler.builder()
            .rename(
                _.path(Path("embedded"))
                    .column("s", Path("structure"))
            )
            .build()

        val expectedSchema = StructType(Seq(
            StructField("s", StructType(Seq(
                StructField("public", StringType),
                StructField("secret", StructType(Seq(
                    StructField("value", StringType)
                )))
            )))
        ))

        val resultDf = asm.reassemble(inputDf)
        resultDf.count should be (1)
        resultDf.schema should be (expectedSchema)

        val resultSchema = asm.reassemble(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "support renaming non-existing columns" in {
        val asm = Assembler.builder()
            .columns(
                _.path(Path("embedded.structure"))
                    .keep(Path("public"))
            )
            .rename(
                _.path(Path("embedded"))
                    .column("s", Path("no_such_element"))
            )
            .build()

        val expectedSchema = StructType(Seq(
            StructField("public", StringType)
        ))

        val resultDf = asm.reassemble(inputDf)
        resultDf.count should be (1)
        resultDf.schema should be (expectedSchema)

        val resultSchema = asm.reassemble(inputSchema)
        resultSchema.sparkType should be (resultDf.schema)
    }

    it should "throw an exception on non-existing path in renam" in {
        val asm = Assembler.builder()
            .rename(
                _.path(Path("embedded.no_such_path"))
            )
            .build()

        an[IllegalArgumentException] shouldBe thrownBy(asm.reassemble(inputDf))
    }
}
