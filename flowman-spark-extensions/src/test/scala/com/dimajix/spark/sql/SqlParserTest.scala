/*
 * Copyright 2019-2023 Kaya Kupferschmidt
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

package com.dimajix.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.SPARK_VERSION


class SqlParserTest extends AnyFlatSpec with Matchers {
    "The SqlParser" should "detect all dependencies" in {
        val deps = SqlParser.resolveDependencies(
            """
              |SELECT * FROM db.lala
              |""".stripMargin)
        deps should be (Set("db.lala"))
    }

    it should "support CTEs" in {
        val deps = SqlParser.resolveDependencies(
            """
              |WITH tmp AS (
              |     SELECT
              |         *
              |     FROM db.lala
              |)
              |SELECT * FROM tmp
              |""".stripMargin)
        deps should be (Set("db.lala"))
    }

    it should "support nested CTEs" in {
        val deps = SqlParser.resolveDependencies(
            """
              |WITH tmp AS (
              |   WITH tmp2 AS (
              |       SELECT
              |          *
              |       FROM db.lala
              |   )
              |   SELECT
              |     *
              |   FROM tmp2
              |)
              |SELECT * FROM tmp
              |""".stripMargin)
        deps should be (Set("db.lala"))
    }

    it should "support scalar expressions" in {
        val sql =
            """
              |SELECT
              |  id AS campaign
              |FROM
              |  fe_campaign
              |WHERE
              |  CAST(end_date AS DATE) >= CAST('${processing_datetime}' AS DATE)
              |    AND CAST(start_date AS DATE) > (SELECT min(day) FROM campaign_contacts_raw)
              |""".stripMargin
        val deps = SqlParser.resolveDependencies(sql)
        deps should be (Set("fe_campaign", "campaign_contacts_raw"))
    }

    it should "support escaped names" in {
        val sql =
            """
              |SELECT
              | *
              |FROM `some_mapping`
              |""".stripMargin
        val deps = SqlParser.resolveDependencies(sql)
        deps should be(Set("some_mapping"))
    }

    it should "support escaped names with special characters" in (if (SPARK_VERSION >= "3") {
        val sql =
            """
              |SELECT
              | *
              |FROM `prj/some_mapping:``output```
              |""".stripMargin
        val deps = SqlParser.resolveDependencies(sql)
        deps should be(Set("prj/some_mapping:`output`"))
    })
}
