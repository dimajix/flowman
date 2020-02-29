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
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.mapping.AssembleMapping.AppendEntry
import com.dimajix.flowman.spec.mapping.AssembleMapping.ExplodeEntry
import com.dimajix.flowman.spec.mapping.AssembleMapping.LiftEntry
import com.dimajix.flowman.spec.mapping.AssembleMapping.NestEntry
import com.dimajix.flowman.spec.mapping.AssembleMapping.RenameEntry
import com.dimajix.flowman.spec.mapping.AssembleMapping.StructEntry
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession


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

    "An Assembler" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  my_structure:
              |    kind: assemble
              |    input: some_mapping
              |    columns:
              |      - # Add several top level columns
              |        kind: append
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
              |      - kind: explode
              |        name: array
              |        path: embedded.struct_array
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val mapping = project.mappings("my_structure")

        mapping shouldBe an[AssembleMappingSpec]
    }

    it should "transform DataFrames correctly" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = AssembleMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Seq(
                NestEntry("clever_name", "stupidName", Seq(), Seq("secret.field")),
                AppendEntry("", Seq("lala", "lolo"), Seq()),
                AppendEntry("", Seq(), Seq("stupidName", "embedded.structure.secret", "embedded.old_structure")),
                StructEntry("sub_structure", Seq(
                    AppendEntry("embedded.old_structure", Seq(), Seq())
                )),
                LiftEntry("stupidName", Seq("secret.field"))
            )
        )

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")

        val expectedSchema = StructType(Seq(
            StructField("clever_name", StructType(Seq(
                StructField("secret", StructType(Seq(
                    StructField("other_field", LongType)
                )))
            ))),
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
            StructField("sub_structure", StructType(Seq(
                StructField("value", ArrayType(LongType))
            ))),
            StructField("field", LongType)
        ))

        outputDf.count should be (1)
        outputDf.schema should be (expectedSchema)
    }

    it should "provide a correct output schema" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = AssembleMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Seq(
                NestEntry("clever_name", "stupidName", Seq(), Seq("secret.field")),
                AppendEntry("", Seq("lala", "lolo"), Seq()),
                AppendEntry("", Seq(), Seq("stupidName", "embedded.structure.secret", "embedded.old_structure")),
                StructEntry("sub_structure", Seq(
                    AppendEntry("embedded.old_structure", Seq(), Seq())
                )),
                LiftEntry("stupidName", Seq("secret.field"))
            )
        )

        val expectedSchema = StructType(Seq(
            StructField("clever_name", StructType(Seq(
                StructField("secret", StructType(Seq(
                    StructField("other_field", LongType)
                )), true)
            )), true),
            StructField("embedded", StructType(Seq(
                StructField("struct_array", ArrayType(
                    StructType(Seq(
                        StructField("key", StringType),
                        StructField("value", LongType)
                    ))
                ), true),
                StructField("structure", StructType(Seq(
                    StructField("public", StringType)
                )), true)
            )), true),
            StructField("sub_structure", StructType(Seq(
                StructField("value", ArrayType(LongType))
            )), true),
            StructField("field", LongType)
        ))

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)), "main")
        outputSchema.sparkType should be (expectedSchema)
    }

    it should "support explodes of complex types with rename" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = AssembleMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Seq(
                ExplodeEntry("array", "embedded.struct_array")
            )
        )

        val expectedSchema = StructType(Seq(
            StructField("array",
                StructType(Seq(
                    StructField("key", StringType),
                    StructField("value", LongType)
                )
            ), true)
        ))

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)), "main")
        outputSchema.sparkType should be (expectedSchema)

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")
        outputDf.schema should be (expectedSchema)
        outputDf.count() should be (2)
    }

    it should "support explodes of complex types without rename" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = AssembleMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Seq(
                ExplodeEntry("embedded.struct_array")
            )
        )

        val expectedSchema = StructType(Seq(
            StructField("struct_array",
                StructType(Seq(
                    StructField("key", StringType),
                    StructField("value", LongType)
                )
                ), true)
        ))

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)), "main")
        outputSchema.sparkType should be (expectedSchema)

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")
        outputDf.schema should be (expectedSchema)
        outputDf.count() should be (2)
    }

    it should "support explodes of simple types" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = AssembleMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Seq(
                ExplodeEntry("embedded.old_structure.value")
            )
        )

        val expectedSchema = StructType(Seq(
            StructField("value", LongType)
        ))

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)), "main")
        outputSchema.sparkType should be (expectedSchema)

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")
        outputDf.schema should be (expectedSchema)
        outputDf.count() should be (2)
    }

    it should "throw exceptions on explodes of non-existing paths" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = AssembleMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Seq(
                ExplodeEntry("no_such_path")
            )
        )

        an[IllegalArgumentException] shouldBe thrownBy(mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema))))
    }

    it should "support rename operations" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = AssembleMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Seq(
                RenameEntry("embedded", Map("new_elem" -> "old_structure"))
            )
        )

        val expectedSchema = StructType(Seq(
            StructField("new_elem", StructType(Seq(
                StructField("value", ArrayType(LongType))
            )))
       ))

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)), "main")
        outputSchema.sparkType should be (expectedSchema)

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")
        outputDf.schema should be (expectedSchema)
        outputDf.count() should be (1)
    }

    it should "not throw an exception on renames of non-existing fields" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = AssembleMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Seq(
                AppendEntry("embedded.structure", Seq("public"), Seq()),
                RenameEntry("embedded", Map("new_elem" -> "no_such_field"))
            )
        )

        val expectedSchema = StructType(Seq(
            StructField("public", StringType)
        ))

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)), "main")
        outputSchema.sparkType should be (expectedSchema)

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")
        outputDf.schema should be (expectedSchema)
        outputDf.count() should be (1)
    }
}
