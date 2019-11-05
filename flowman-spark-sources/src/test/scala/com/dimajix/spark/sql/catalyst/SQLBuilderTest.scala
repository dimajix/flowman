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

package com.dimajix.spark.sql.catalyst

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.spark.testing.LocalSparkSession


class SQLBuilderTest extends FlatSpec with Matchers with LocalSparkSession {

    override def beforeAll(): Unit = {
        super.beforeAll()

        if (hiveSupported) {
            spark.sql(
                """
                  CREATE TABLE sql_builder_0(
                    col_0 INT,
                    col_1 STRING,
                    ts TIMESTAMP
                  )
                """)
            spark.sql(
                """
                  |CREATE VIEW sql_builder_1 AS
                  |SELECT * FROM sql_builder_0
                  |""".stripMargin
            )
        }
    }

    override def afterAll(): Unit = {
        if (hiveSupported) {
            spark.sql("DROP TABLE sql_builder_0")
        }

        super.afterAll()
    }

    behavior of "The SQLBuilder"

    it should "support basic selects" in (if (hiveSupported) {
        val df1 = spark.sql("SELECT * FROM sql_builder_0")
        val sql1 = new SQLBuilder(df1.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql1))
        sql1 should be ("SELECT `col_0`, `col_1`, `ts` FROM `default`.`sql_builder_0`")

        val df2 = spark.sql("SELECT CONCAT(col_0, col_1) AS result FROM sql_builder_0")
        val sql2 = new SQLBuilder(df2.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql2))
        sql2 should be ("SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `result` FROM `default`.`sql_builder_0`")

        val df3 = spark.sql("SELECT CONCAT(x.col_0, x.col_1) AS result FROM sql_builder_0 AS x")
        val sql3 = new SQLBuilder(df3.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql3))
        sql3 should be ("SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `result` FROM `default`.`sql_builder_0`")

        val df4 = spark.sql("SELECT CONCAT(x.col_0, x.col_1) AS result FROM sql_builder_0 AS x WHERE x.col_0 = 67")
        val sql4 = new SQLBuilder(df4.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql4))
        sql4 should be ("SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `result` FROM `default`.`sql_builder_0` WHERE (`col_0` = 67)")
    })

    it should "support VIEWs" in (if (hiveSupported) {
        val df1 = spark.sql("SELECT * FROM sql_builder_1")
        val sql1 = new SQLBuilder(df1.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql1))
        sql1 should be ("SELECT `col_0`, `col_1`, `ts` FROM `default`.`sql_builder_1`")
    })

    it should "support JOINs" in (if (hiveSupported) {
        val df5 = spark.sql(
            """
              |SELECT
              |   CONCAT(x.col_0, y.col_1) AS result
              |FROM sql_builder_0 AS x
              |JOIN  sql_builder_0 AS y
              |  ON x.col_0 = y.col_0
              |WHERE x.col_1 = '67'""".stripMargin)
        val sql5 = new SQLBuilder(df5.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql5))
        sql5 should be ("SELECT concat(CAST(`gen_attr_1` AS STRING), `gen_attr_2`) AS `result` FROM (SELECT `col_0` AS `gen_attr_1`, `col_1` AS `gen_attr_3`, `ts` AS `gen_attr_5` FROM `default`.`sql_builder_0`) AS gen_subquery_0 INNER JOIN (SELECT `col_0` AS `gen_attr_4`, `col_1` AS `gen_attr_2`, `ts` AS `gen_attr_6` FROM `default`.`sql_builder_0`) AS gen_subquery_1 ON (`gen_attr_1` = `gen_attr_4`) WHERE (`gen_attr_3` = '67')")
    })

    it should "support UNIONs" in (if (hiveSupported) {
       val df6 = spark.sql(
            """
              |SELECT
              |   CONCAT(x.col_0, x.col_1) AS result
              |FROM sql_builder_0 AS x
              |
              |UNION ALL
              |
              |SELECT
              |   CONCAT(x.col_0, x.col_1) AS result
              |FROM sql_builder_0 AS x
              |WHERE x.col_1 = '67'""".stripMargin)
        val sql6 = new SQLBuilder(df6.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql6))
        sql6 should be ("(SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `result` FROM `default`.`sql_builder_0`) UNION ALL (SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `result` FROM `default`.`sql_builder_0` WHERE (`col_1` = '67'))")
    })

    it should "support WINDOW functions" in (if (hiveSupported) {
        val df7 = spark.sql(
            """
              |SELECT
              |  x.col_0,
              |  x.col_1
              |FROM (
              |  SELECT
              |     col_0,
              |     col_1,
              |     row_number() OVER(PARTITION BY col_0 ORDER BY ts) AS rank
              |  FROM sql_builder_0
              |) AS x
              |WHERE x.rank = 1""".stripMargin)
        val sql7 = new SQLBuilder(df7.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql7))
        sql7 should be ("SELECT `gen_attr_0` AS `col_0`, `gen_attr_1` AS `col_1` FROM (SELECT `col_0` AS `gen_attr_0`, `col_1` AS `gen_attr_1`, row_number() OVER (PARTITION BY `col_0` ORDER BY `ts` ASC NULLS FIRST) AS `gen_attr_2` FROM `default`.`sql_builder_0`) AS x WHERE (`gen_attr_2` = 1)")

        val df8 = spark.sql(
            """
              |SELECT
              |   col_0,
              |   col_1,
              |   lead(col_1, 1) OVER(PARTITION BY col_0 ORDER BY ts) AS rank
              |FROM sql_builder_0
              |""".stripMargin)
        val sql8 = new SQLBuilder(df8.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql8))
        sql8 should be ("SELECT `col_0`, `col_1`, lead(`col_1`, 1, NULL) OVER (PARTITION BY `col_0` ORDER BY `ts` ASC NULLS FIRST) AS `rank` FROM `default`.`sql_builder_0`")

        val df9 = spark.sql(
            """
              |SELECT
              |  x.col_0,
              |  x.col_1
              |FROM (
              |  SELECT
              |     col_0,
              |     col_1,
              |     max(col_0) OVER(PARTITION BY col_1 ORDER BY ts ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rank
              |  FROM sql_builder_0
              |) AS x
              |WHERE x.rank = 1""".stripMargin)
        val sql9 = new SQLBuilder(df9.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql9))
        sql9 should be ("SELECT `gen_attr_0` AS `col_0`, `gen_attr_1` AS `col_1` FROM (SELECT `col_0` AS `gen_attr_0`, `col_1` AS `gen_attr_1`, max(`col_0`) OVER (PARTITION BY `col_1` ORDER BY `ts` ASC NULLS FIRST ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS `gen_attr_2` FROM `default`.`sql_builder_0`) AS x WHERE (`gen_attr_2` = 1)")
    })
}
