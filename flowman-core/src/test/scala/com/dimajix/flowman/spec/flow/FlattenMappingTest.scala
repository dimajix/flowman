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

package com.dimajix.flowman.spec.flow

import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.testing.LocalSparkSession
import com.dimajix.flowman.{types => ftypes}


class FlattenMappingTest extends FlatSpec with Matchers with LocalSparkSession{
    "A FlattenMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  my_structure:
              |    kind: flatten
              |    naming: camelCase
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val mapping = project.mappings("my_structure")

        mapping shouldBe an[FlattenMapping]
    }

    it should "flatten nested structures" in {
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

        val mapping = FlattenMapping("input_df", "snakeCase")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val expectedSchema = StructType(Seq(
            StructField("stupid_name_secret_struct_other_field", LongType),
            StructField("stupid_name_secret_struct_secret_field", LongType)
        ))

        val outputDf = mapping.execute(executor, Map(MappingIdentifier("input_df") -> inputDf))
        outputDf.schema should be (expectedSchema)

        val outputSchema = mapping.describe(executor.context, Map(MappingIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)))
        outputSchema.sparkType should be (expectedSchema)
    }
}
