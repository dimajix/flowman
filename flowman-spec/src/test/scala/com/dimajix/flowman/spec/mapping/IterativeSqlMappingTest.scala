/*
 * Copyright 2020 Kaya Kupferschmidt
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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.ExecutionException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class IterativeSqlMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession{
    "The IterativeSqlMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  t1:
              |    kind: iterativeSql
              |    input: some_input
              |    maxIterations: 12
              |    sql: "
              |      SELECT x,y
              |      FROM t0
              |      "
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        project.mappings.size should be (1)
        project.mappings.contains("t1") should be (true)
        project.mappings("t1") shouldBe a[IterativeSqlMappingSpec]

        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)
        val mapping = context.getMapping(MappingIdentifier("t1"))
        mapping shouldBe a[IterativeSqlMapping]
    }

    it should "calculate factorials" in {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val executor = session.execution

        val mapping = IterativeSqlMapping(
            Mapping.Properties(context),
            MappingOutputIdentifier("input"),
            Some("""
              |SELECT
              |     IF(n < 6, n+1, n) AS n
              |FROM __this__
              |""".stripMargin),
            None,
            None
        )

        val inputDf = spark.createDataFrame(Seq((1,1))).withColumnRenamed("_1", "n")
        val resultDf = mapping.execute(executor, Map(MappingOutputIdentifier("input") -> inputDf))("main")
        val resultRecords = resultDf.as[Int].collect()
        resultRecords should be (Array(6))

        val resultSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input") -> StructType.of(inputDf.schema)))
        resultSchema should be (Map(
            "main" -> StructType(Seq(
                Field("n", IntegerType, false)
            ))
        ))
    }

    it should "throw an exception on too many iterations" in {
        val spark = this.spark

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val executor = session.execution

        val mapping = IterativeSqlMapping(
            Mapping.Properties(context),
            MappingOutputIdentifier("input"),
            Some(
                """
                  |SELECT
                  |     n+1 AS n
                  |FROM __this__
                  |""".stripMargin),
            None,
            None,
            maxIterations = 2
        )

        val inputDf = spark.createDataFrame(Seq((1,1))).withColumnRenamed("_1", "n")
        an[ExecutionException] should be thrownBy(mapping.execute(executor, Map(MappingOutputIdentifier("input") -> inputDf))("main"))
        val resultSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input") -> StructType.of(inputDf.schema)))
        resultSchema should be(Map(
            "main" -> StructType(Seq(
                Field("n", IntegerType, false)
            ))
        ))
    }
}
