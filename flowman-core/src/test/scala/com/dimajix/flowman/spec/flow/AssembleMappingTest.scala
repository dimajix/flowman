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

package com.dimajix.flowman.spec.flow

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.flow.AssembleMapping.ColumnsEntry
import com.dimajix.flowman.spec.flow.AssembleMapping.LiftEntry
import com.dimajix.flowman.spec.flow.AssembleMapping.NestEntry
import com.dimajix.flowman.spec.flow.AssembleMapping.StructEntry


class AssembleMappingTest extends FlatSpec with Matchers with LocalSparkSession {
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

    "An Assembler" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  my_structure:
              |    kind: assemble
              |    input: some_mapping
              |    columns:
              |      - # Add several top level columns
              |        kind: columns
              |        keep:
              |         - lala
              |         - lolo
              |         - lolo.subentry.subsub
              |         - lolo.subentry.subsub2
              |        drop:
              |         - embedded.structure.secret
              |         - embedded.old_structure
              |
              |      - # Rename columns stupidName to column clever_name
              |        kind: nest
              |        name: clever_name
              |        path: stupidName
              |
              |      - # Rename struct stupidName struct to clever_name and drop stupidName.secret.field
              |        kind: nest
              |        name: clever_name
              |        path: stupidName
              |        drop: secret.field
              |
              |      - # Rename struct stupidName struct to clever_name and drop stupidName.secret.field
              |        kind: struct
              |        name: clever_name
              |        columns:
              |          - path: stupidName
              |            drop: secret.field
              |
              |      - kind: struct
              |        name: sub_structure
              |        columns:
              |          - path: embedded.old_structure
              |            drop: secret2
              |
              |          - kind: struct
              |            name: structure
              |            columns:
              |              - kind: columns
              |                path: embedded.structure
              |                drop: secret
              |
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val mapping = project.mappings("my_structure")

        mapping shouldBe an[AssembleMapping]
    }

    it should "transform DataFrames correctly" in {
        val spark = this.spark
        import spark.implicits._

        val mapping = AssembleMapping(
            "input_df",
            Seq(
                NestEntry("clever_name", "stupidName", Seq(), Seq("secret.field")),
                ColumnsEntry("", Seq("lala", "lolo"), Seq()),
                ColumnsEntry("", Seq(), Seq("stupidName", "embedded.structure.secret", "embedded.old_structure")),
                StructEntry("sub_structure", Seq(
                    ColumnsEntry("embedded.old_structure", Seq(), Seq())
                )),
                LiftEntry("stupidName", Seq("secret.field"))
            )
        )

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        implicit val context = executor.context

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        val inputDf = spark.read.json(inputDs)

        val outputDf = mapping.execute(executor, Map(MappingIdentifier("input_df") -> inputDf))

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
            )), false),
            StructField("field", LongType)
        ))

        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)
    }

    it should "provide a correct output schema" in {
        val spark = this.spark
        import spark.implicits._

        val mapping = AssembleMapping(
            "input_df",
            Seq(
                NestEntry("clever_name", "stupidName", Seq(), Seq("secret.field")),
                ColumnsEntry("", Seq("lala", "lolo"), Seq()),
                ColumnsEntry("", Seq(), Seq("stupidName", "embedded.structure.secret", "embedded.old_structure")),
                StructEntry("sub_structure", Seq(
                    ColumnsEntry("embedded.old_structure", Seq(), Seq())
                )),
                LiftEntry("stupidName", Seq("secret.field"))
            )
        )

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        implicit val context = executor.context

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        val inputDf = spark.read.json(inputDs)

        val expectedSchema = StructType(Seq(
            StructField("clever_name", StructType(Seq(
                StructField("secret", StructType(Seq(
                    StructField("other_field", LongType)
                )), true)
            )), true),
            StructField("embedded", StructType(Seq(
                StructField("structure", StructType(Seq(
                    StructField("public", StringType)
                )), true)
            )), true),
            StructField("sub_structure", StructType(Seq(
                StructField("value", ArrayType(LongType))
            )), true),
            StructField("field", LongType)
        ))

        val outputSchema = mapping.describe(context, Map(MappingIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)))
        outputSchema.sparkType should be (expectedSchema)
    }
}
