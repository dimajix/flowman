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

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class RecursiveSqlMappingTest extends FlatSpec with Matchers with LocalSparkSession{
    "The RecursiveSqlMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  t1:
              |    kind: recursiveSql
              |    sql: "
              |      SELECT x,y
              |      FROM t0
              |      "
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        project.mappings.size should be (1)
        project.mappings.contains("t1") should be (true)
        project.mappings("t1") shouldBe a[RecursiveSqlMappingSpec]

        val session = Session.builder().withProject(project).build()
        val context = session.getContext(project)
        val mapping = context.getMapping(MappingIdentifier("t1"))
        mapping shouldBe a[RecursiveSqlMapping]
    }

    it should "calculate factorials" in {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val executor = session.executor

        val mapping = RecursiveSqlMapping(
            Mapping.Properties(context),
            """
              |SELECT
              |     0 AS n,
              |     1 AS fact
              |
              |UNION ALL
              |
              |SELECT
              |     n+1 AS n,
              |     (n+1)*fact AS fact
              |FROM __this__
              |WHERE n < 6
              |""".stripMargin,
            null,
            null
        )

        val resultDf = mapping.execute(executor, Map())("main")
        val resultRecords = resultDf.as[(Int,Int)].collect()
        resultRecords should be (Array(
            (0,1),
            (1,1),
            (2,2),
            (3,6),
            (4,24),
            (5,120),
            (6,720)
        ))

        val resultSchema = mapping.describe(executor, Map())
        resultSchema should be (Map(
            "main" -> StructType(Seq(
                Field("n", IntegerType, false),
                Field("fact", IntegerType, false)
            ))
        ))
    }

    it should "support UNION DISTINCT" in {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val executor = session.executor

        val mapping = RecursiveSqlMapping(
            Mapping.Properties(context),
            """
              |SELECT
              |     0 AS n,
              |     1 AS fact
              |
              |UNION DISTINCT
              |
              |SELECT
              |     n+1 AS n,
              |     (n+1)*fact AS fact
              |FROM __this__
              |WHERE n < 6
              |""".stripMargin,
            null,
            null
        )

        val resultDf = mapping.execute(executor, Map())("main")
        val resultRecords = resultDf.orderBy("n").as[(Int,Int)].collect()
        resultRecords should be (Array(
            (0,1),
            (1,1),
            (2,2),
            (3,6),
            (4,24),
            (5,120),
            (6,720)
        ))

        val resultSchema = mapping.describe(executor, Map())
        resultSchema should be (Map(
            "main" -> StructType(Seq(
                Field("n", IntegerType, false),
                Field("fact", IntegerType, false)
            ))
        ))
    }
}
