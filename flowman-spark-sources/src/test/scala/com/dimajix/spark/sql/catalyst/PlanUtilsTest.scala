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

package com.dimajix.spark.sql.catalyst

import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.spark.sql.SqlParser
import com.dimajix.spark.testing.LocalSparkSession


class PlanUtilsTest extends FlatSpec with Matchers with LocalSparkSession {
    "Replacing dependencies" should "work" in {
        val sql =
            """
              |SELECT * FROM lala
              |""".stripMargin
        val plan = SqlParser.parsePlan(sql)

        val replacements = Map(
            "lala" -> StructType(Seq(
                StructField("f1", StringType),
                StructField("f2", LongType)
            ))
        )

        val replacedPlan = PlanUtils.replaceDependencies(plan, replacements)
        val finalPlan = SimpleAnalyzer.execute(replacedPlan)
        finalPlan.output.map(att => (att.name, att.dataType)) should be (Seq(
            "f1" -> StringType,
            "f2" -> LongType
        ))
    }

    it should "work with nested selects" in {
        val sql =
            """
              |WITH tmp AS (
              |     SELECT
              |         *
              |     FROM lala
              |)
              |SELECT
              |     *,
              |     concat(f1,f2) AS c
              |FROM tmp
              |""".stripMargin
        val plan = SqlParser.parsePlan(sql)

        val replacements = Map(
            "lala" -> StructType(Seq(
                StructField("f1", StringType),
                StructField("f2", LongType)
            ))
        )

        val replacedPlan = PlanUtils.replaceDependencies(plan, replacements)
        val finalPlan = PlanUtils.analyze(spark, replacedPlan)
        finalPlan.output.map(att => (att.name, att.dataType)) should be (Seq(
            "f1" -> StringType,
            "f2" -> LongType,
            "c" -> StringType
        ))
    }

}
