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

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.testing.LocalSparkSession
import com.dimajix.flowman.{types => ftypes}


class DropMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    "The DropMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  drop:
              |    kind: drop
              |    input: dummy
              |    columns:
              |      - col_1
              |      - col-2
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.mappings.keys should contain("drop")
        project.mappings("drop") shouldBe a[DropMappingSpec]
    }

    it should "drop known columns" in {
        val sparkSession = spark
        import sparkSession.implicits._

        val records = Seq(
            ("row_1", "col2", "col3", "col4")
        )
        val inputDf = records.toDF
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = DropMapping("df", Seq("_1", "_3"))
        val expectedSchema = StructType(Seq(
            StructField("_2", StringType),
            StructField("_4", StringType)
        ))

        val outputDf = mapping.execute(executor, Map(MappingIdentifier("df") -> inputDf))
        outputDf.schema should be (expectedSchema)

        val outputSchema = mapping.describe(Map(MappingIdentifier("df") -> ftypes.StructType.of(inputDf.schema)))
        outputSchema.sparkType should be (expectedSchema)
    }

    it should "ignore non-existing columns" in {
        val sparkSession = spark
        import sparkSession.implicits._

        val records = Seq(
            ("row_1", "col2", "col3", "col4")
        )
        val inputDf = records.toDF
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = DropMapping("df", Seq("_1", "_3", "no_such_column"))
        val expectedSchema = StructType(Seq(
            StructField("_2", StringType),
            StructField("_4", StringType)
        ))

        val outputDf = mapping.execute(executor, Map(MappingIdentifier("df") -> inputDf))
        outputDf.schema should be (expectedSchema)

        val outputSchema = mapping.describe(Map(MappingIdentifier("df") -> ftypes.StructType.of(inputDf.schema)))
        outputSchema.sparkType should be (expectedSchema)
    }
}
