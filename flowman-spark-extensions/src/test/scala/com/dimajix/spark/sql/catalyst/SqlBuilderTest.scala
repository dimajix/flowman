/*
 * Copyright (C) 2018-2024 The Flowman Authors
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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.features.hiveVarcharSupported
import com.dimajix.spark.testing.LocalSparkSession


class SqlBuilderTest extends AnyFlatSpec with Matchers with LocalSparkSession {
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

        if (hiveSupported && hiveVarcharSupported) {
            spark.sql(
                """
                  CREATE TABLE sql_builder_2(
                    col_0 CHAR(10),
                    col_1 VARCHAR(20)
                  )
                """)
        }
    }

    override def afterAll(): Unit = {
        if (hiveSupported) {
            spark.sql("DROP TABLE sql_builder_0")
        }

        super.afterAll()
    }

    behavior of "The SqlBuilder"

    it should "support basic selects" in (if (hiveSupported) {
        val df1 = spark.sql("SELECT * FROM sql_builder_0")
        val sql1 = new SqlBuilder(df1.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql1))
        sql1 should be ("SELECT `col_0`, `col_1`, `ts` FROM `default`.`sql_builder_0`")

        val df2 = spark.sql("SELECT CONCAT(col_0, col_1) AS result FROM sql_builder_0")
        val sql2 = new SqlBuilder(df2.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql2))
        sql2 should be ("SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `result` FROM `default`.`sql_builder_0`")

        val df3 = spark.sql("SELECT CONCAT(x.col_0, x.col_1) AS result FROM sql_builder_0 AS x")
        val sql3 = new SqlBuilder(df3.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql3))
        sql3 should be ("SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `result` FROM `default`.`sql_builder_0`")

        val df4 = spark.sql("SELECT CONCAT(x.col_0, x.col_1) AS result FROM sql_builder_0 AS x WHERE x.col_0 = 67")
        val sql4 = new SqlBuilder(df4.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql4))
        sql4 should be ("SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `result` FROM `default`.`sql_builder_0` WHERE (`col_0` = 67)")
    })

    it should "support basic arithmetics" in (if (hiveSupported) {
        val df1 = spark.sql("SELECT 1+2")
        val sql1 = new SqlBuilder(df1.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql1))
        sql1 should be("SELECT (1 + 2) AS `(1 + 2)`")

        val df2 = spark.sql("SELECT 1+2 AS result")
        val sql2 = new SqlBuilder(df2.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql2))
        sql2 should be("SELECT (1 + 2) AS `result`")

        val df3 = spark.sql("SELECT -3*(1+2)")
        val sql3 = new SqlBuilder(df3.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql3))
        sql3 should be("SELECT (-3 * (1 + 2)) AS `(-3 * (1 + 2))`")

        val df4 = spark.sql("SELECT -(8*3)-(1+2)")
        val sql4 = new SqlBuilder(df4.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql4))
        sql4 should be("SELECT ((- (8 * 3)) - (1 + 2)) AS `((- (8 * 3)) - (1 + 2))`")
    })

    it should "support VIEWs" in (if (hiveSupported) {
        val df1 = spark.sql("SELECT * FROM sql_builder_1")
        val sql1 = new SqlBuilder(df1.queryExecution.analyzed).toSQL
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
        val sql5 = new SqlBuilder(df5.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql5))
        sql5 should be ("SELECT concat(CAST(`gen_attr_1` AS STRING), `gen_attr_2`) AS `result` FROM (SELECT `col_0` AS `gen_attr_1`, `col_1` AS `gen_attr_3`, `ts` AS `gen_attr_5` FROM `default`.`sql_builder_0`) AS `gen_subquery_0` INNER JOIN (SELECT `col_0` AS `gen_attr_4`, `col_1` AS `gen_attr_2`, `ts` AS `gen_attr_6` FROM `default`.`sql_builder_0`) AS `gen_subquery_1` ON (`gen_attr_1` = `gen_attr_4`) WHERE (`gen_attr_3` = '67')")
    })

    it should "support UNIONs without column names" in (if (hiveSupported) {
        val df3 = spark.sql(
            """
              |SELECT
              |   CONCAT(x.col_0, x.col_1)
              |FROM sql_builder_0 AS x
              |
              |UNION ALL
              |
              |SELECT
              |   CONCAT(x.col_0, x.col_1)
              |FROM sql_builder_0 AS x
              |WHERE x.col_1 = '67'""".stripMargin)
        val sql3 = new SqlBuilder(df3.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql3))
        if (spark.version < "3.2")
            sql3 should be("SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `concat(CAST(col_0 AS STRING), col_1)` FROM `default`.`sql_builder_0` UNION ALL SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `concat(CAST(col_0 AS STRING), col_1)` FROM `default`.`sql_builder_0` WHERE (`col_1` = '67')")
        else
            sql3 should be("SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `concat(col_0, col_1)` FROM `default`.`sql_builder_0` UNION ALL SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `concat(col_0, col_1)` FROM `default`.`sql_builder_0` WHERE (`col_1` = '67')")
    })

    it should "support UNIONs with one attribute name" in (if (hiveSupported) {
        val df4 = spark.sql(
            """
              |SELECT
              |   CONCAT(x.col_0, x.col_1) AS result
              |FROM sql_builder_0 AS x
              |
              |UNION ALL
              |
              |SELECT
              |   CONCAT(x.col_0, x.col_1)
              |FROM sql_builder_0 AS x
              |WHERE x.col_1 = '67'""".stripMargin)
        val sql4 = new SqlBuilder(df4.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql4))
        sql4 should be ("SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `result` FROM `default`.`sql_builder_0` UNION ALL SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `result` FROM `default`.`sql_builder_0` WHERE (`col_1` = '67')")
    })

    it should "support UNIONs with different column names" in (if (hiveSupported) {
        val df5 = spark.sql(
            """
              |SELECT
              |   CONCAT(x.col_0, x.col_1) AS result
              |FROM sql_builder_0 AS x
              |
              |UNION ALL
              |
              |SELECT
              |   CONCAT(x.col_0, x.col_1) AS result_2
              |FROM sql_builder_0 AS x
              |WHERE x.col_1 = '67'""".stripMargin)
        val sql5 = new SqlBuilder(df5.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql5))
        sql5 should be ("SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `result` FROM `default`.`sql_builder_0` UNION ALL SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `result` FROM `default`.`sql_builder_0` WHERE (`col_1` = '67')")
    })

    it should "support UNIONs with matching column names" in (if (hiveSupported) {
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
        val sql6 = new SqlBuilder(df6.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql6))
        sql6 should be ("SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `result` FROM `default`.`sql_builder_0` UNION ALL SELECT concat(CAST(`col_0` AS STRING), `col_1`) AS `result` FROM `default`.`sql_builder_0` WHERE (`col_1` = '67')")
    })

    it should "support subquery UNIONs" in (if (hiveSupported) {
        val df1 = spark.sql(
            """
              |SELECT
              |    gen_attr_1 AS first,
              |    gen_attr_3 AS second
              |FROM (
              |        SELECT
              |            col_0 AS gen_attr_1,
              |            col_1 AS gen_attr_3
              |        FROM sql_builder_0
              |        UNION ALL
              |        SELECT
              |            col_0 AS gen_attr_11,
              |            col_1 AS gen_attr_31
              |        FROM sql_builder_0
              |) AS gen_subquery_2
              |WHERE gen_attr_1 != 7
              |""".stripMargin
        )
        val sql1 = new SqlBuilder(df1.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql1))
        sql1 should be ("SELECT `gen_attr_1` AS `first`, `gen_attr_3` AS `second` FROM (SELECT `col_0` AS `gen_attr_1`, `col_1` AS `gen_attr_3` FROM `default`.`sql_builder_0` UNION ALL SELECT `col_0` AS `gen_attr_1`, `col_1` AS `gen_attr_3` FROM `default`.`sql_builder_0`) AS `gen_subquery_2` WHERE (NOT (`gen_attr_1` = 7))")
    })

    it should "support subquery UNIONs with partial column names" in (if (hiveSupported) {
        val df2 = spark.sql(
            """
              |SELECT
              |    gen_attr_1 AS first,
              |    col_1 AS second
              |FROM (
              |        SELECT
              |            col_0 AS gen_attr_1,
              |            col_1
              |        FROM sql_builder_0
              |        UNION ALL
              |        SELECT
              |            upper(col_0),
              |            col_1 AS gen_attr_31
              |        FROM sql_builder_0
              |) AS gen_subquery_2
              |""".stripMargin
        )
        val sql2 = new SqlBuilder(df2.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql2))
        sql2 should be ("SELECT CAST(`col_0` AS STRING) AS `first`, `col_1` AS `second` FROM `default`.`sql_builder_0` UNION ALL SELECT upper(CAST(`col_0` AS STRING)) AS `first`, `col_1` AS `second` FROM `default`.`sql_builder_0`")
    })

    it should "support subquery UNIONs with partial column names and filters" in (if (hiveSupported) {
        val df2 = spark.sql(
            """
              |SELECT
              |    gen_attr_1 AS first,
              |    col_1 AS second
              |FROM (
              |        SELECT
              |            col_0 AS gen_attr_1,
              |            col_1
              |        FROM sql_builder_0
              |        UNION ALL
              |        SELECT
              |            upper(col_0),
              |            col_1 AS gen_attr_31
              |        FROM sql_builder_0
              |) AS gen_subquery_2
              |WHERE gen_attr_1 != 7
              |""".stripMargin
        )
        val sql2 = new SqlBuilder(df2.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql2))
        sql2 should be ("SELECT `gen_attr_1` AS `first`, `gen_attr_3` AS `second` FROM (SELECT CAST(`col_0` AS STRING) AS `gen_attr_1`, `col_1` AS `gen_attr_3` FROM `default`.`sql_builder_0` UNION ALL SELECT upper(CAST(`col_0` AS STRING)) AS `gen_attr_1`, `col_1` AS `gen_attr_3` FROM `default`.`sql_builder_0`) AS `gen_subquery_2` WHERE (NOT (CAST(`gen_attr_1` AS INT) = 7))")
    })

    it should "support subquery UNIONs without alias" in (if (hiveSupported) {
        val df3 = spark.sql(
            """
              |SELECT
              |    gen_attr_1 AS first,
              |    col_1 AS second
              |FROM (
              |        SELECT
              |            col_0 AS gen_attr_1,
              |            col_1
              |        FROM sql_builder_0
              |        UNION ALL
              |        SELECT
              |            upper(col_0),
              |            col_1 AS gen_attr_31
              |        FROM sql_builder_0
              |)
              |""".stripMargin
        )
        val sql3 = new SqlBuilder(df3.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql3))
        sql3 should be ("SELECT CAST(`col_0` AS STRING) AS `first`, `col_1` AS `second` FROM `default`.`sql_builder_0` UNION ALL SELECT upper(CAST(`col_0` AS STRING)) AS `first`, `col_1` AS `second` FROM `default`.`sql_builder_0`")
    })

    it should "support subquery WINDOW functions" in (if (hiveSupported) {
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
        val sql7 = new SqlBuilder(df7.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql7))
        sql7 should be ("SELECT `gen_attr_0` AS `col_0`, `gen_attr_1` AS `col_1` FROM (SELECT `col_0` AS `gen_attr_0`, `col_1` AS `gen_attr_1`, row_number() OVER (PARTITION BY `col_0` ORDER BY `ts` ASC) AS `gen_attr_2` FROM `default`.`sql_builder_0`) AS `x` WHERE (`gen_attr_2` = 1)")
    })

    it should "support plain WINDOW functions" in (if (hiveSupported) {
        val df8 = spark.sql(
            """
              |SELECT
              |   col_0,
              |   col_1,
              |   lead(col_1, 1) OVER(PARTITION BY col_0 ORDER BY ts) AS rank
              |FROM sql_builder_0
              |""".stripMargin)
        val sql8 = new SqlBuilder(df8.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql8))
        sql8 should be ("SELECT `col_0`, `col_1`, lead(`col_1`, 1, NULL) OVER (PARTITION BY `col_0` ORDER BY `ts` ASC) AS `rank` FROM `default`.`sql_builder_0`")
    })

    it should "support subquery WINDOW functions with range" in (if (hiveSupported) {
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
        val sql9 = new SqlBuilder(df9.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql9))
        sql9 should be ("SELECT `gen_attr_0` AS `col_0`, `gen_attr_1` AS `col_1` FROM (SELECT `col_0` AS `gen_attr_0`, `col_1` AS `gen_attr_1`, max(`col_0`) OVER (PARTITION BY `col_1` ORDER BY `ts` ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS `gen_attr_2` FROM `default`.`sql_builder_0`) AS `x` WHERE (`gen_attr_2` = 1)")
    })

    it should "support subquery WINDOW functions with DESC NULLS FIRST" in (if (hiveSupported) {
        val df10 = spark.sql(
            """
              |SELECT
              |  x.col_0,
              |  x.col_1
              |FROM (
              |  SELECT
              |     col_0,
              |     col_1,
              |     max(col_0) OVER(PARTITION BY col_1 ORDER BY ts DESC NULLS FIRST ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rank
              |  FROM sql_builder_0
              |) AS x
              |WHERE x.rank = 1""".stripMargin)
        val sql10 = new SqlBuilder(df10.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql10))
        sql10 should be ("SELECT `gen_attr_0` AS `col_0`, `gen_attr_1` AS `col_1` FROM (SELECT `col_0` AS `gen_attr_0`, `col_1` AS `gen_attr_1`, max(`col_0`) OVER (PARTITION BY `col_1` ORDER BY `ts` DESC NULLS FIRST ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS `gen_attr_2` FROM `default`.`sql_builder_0`) AS `x` WHERE (`gen_attr_2` = 1)")
    })

    it should "support GROUP BY expressions" in (if (hiveSupported) {
        val df8 = spark.sql(
            """
              |SELECT
              |   col_1,
              |   sum(col_0) AS sum
              |FROM sql_builder_0
              |GROUP BY col_1
              |""".stripMargin)
        val sql8 = new SqlBuilder(df8.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql8))
        if (spark.version < "3.2")
            sql8 should be ("SELECT `gen_attr_0` AS `col_1`, sum(CAST(`gen_attr_2` AS BIGINT)) AS `sum` FROM (SELECT `col_0` AS `gen_attr_2`, `col_1` AS `gen_attr_0`, `ts` AS `gen_attr_3` FROM `default`.`sql_builder_0`) AS `gen_subquery_0` GROUP BY `gen_attr_0`")
        else
            sql8 should be ("SELECT `gen_attr_0` AS `col_1`, sum(`gen_attr_2`) AS `sum` FROM (SELECT `col_0` AS `gen_attr_2`, `col_1` AS `gen_attr_0`, `ts` AS `gen_attr_3` FROM `default`.`sql_builder_0`) AS `gen_subquery_0` GROUP BY `gen_attr_0`")

    })

    it should "support GROUP BY expressions with cardinal expressions" in (if (hiveSupported) {
        val df8 = spark.sql(
            """
              |SELECT
              |   col_1,
              |   sum(col_0) AS sum
              |FROM sql_builder_0
              |GROUP BY 1
              |""".stripMargin)
        val sql8 = new SqlBuilder(df8.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql8))
        if (spark.version < "3.2")
            sql8 should be ("SELECT `gen_attr_0` AS `col_1`, sum(CAST(`gen_attr_2` AS BIGINT)) AS `sum` FROM (SELECT `col_0` AS `gen_attr_2`, `col_1` AS `gen_attr_0`, `ts` AS `gen_attr_3` FROM `default`.`sql_builder_0`) AS `gen_subquery_0` GROUP BY `gen_attr_0`")
        else
            sql8 should be ("SELECT `gen_attr_0` AS `col_1`, sum(`gen_attr_2`) AS `sum` FROM (SELECT `col_0` AS `gen_attr_2`, `col_1` AS `gen_attr_0`, `ts` AS `gen_attr_3` FROM `default`.`sql_builder_0`) AS `gen_subquery_0` GROUP BY `gen_attr_0`")
    })

    it should "support sort ordering" in (if (hiveSupported) {
        val df1 = spark.sql(
            """
              |SELECT
              |  x.col_0,
              |  x.col_1
              |FROM sql_builder_0 x
              |ORDER BY col_0""".stripMargin)
        val sql1 = new SqlBuilder(df1.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql1))
        sql1 should be ("SELECT `gen_attr_0` AS `col_0`, `gen_attr_1` AS `col_1` FROM (SELECT `col_0` AS `gen_attr_0`, `col_1` AS `gen_attr_1` FROM `default`.`sql_builder_0`) AS `gen_subquery_1` ORDER BY `gen_attr_0` ASC")
    })

    it should "support sort ordering with cardinal expressions" in (if (hiveSupported) {
        val df1 = spark.sql(
            """
              |SELECT
              |  x.col_0,
              |  x.col_1
              |FROM sql_builder_0 x
              |ORDER BY 1""".stripMargin)
        val sql1 = new SqlBuilder(df1.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql1))
        sql1 should be ("SELECT `gen_attr_0` AS `col_0`, `gen_attr_1` AS `col_1` FROM (SELECT `col_0` AS `gen_attr_0`, `col_1` AS `gen_attr_1` FROM `default`.`sql_builder_0`) AS `gen_subquery_1` ORDER BY `gen_attr_0` ASC")
    })

    it should "support sort ordering with DESC NULLS FIRST" in (if (hiveSupported) {
        val df2 = spark.sql(
            """
              |SELECT
              |  x.col_0,
              |  x.col_1
              |FROM sql_builder_0 x
              |ORDER BY col_0 DESC NULLS FIRST""".stripMargin)
        val sql2 = new SqlBuilder(df2.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql2))
        sql2 should be ("SELECT `gen_attr_0` AS `col_0`, `gen_attr_1` AS `col_1` FROM (SELECT `col_0` AS `gen_attr_0`, `col_1` AS `gen_attr_1` FROM `default`.`sql_builder_0`) AS `gen_subquery_1` ORDER BY `gen_attr_0` DESC NULLS FIRST")
    })

    it should "support VARCHAR(n) and CHAR(n) columns" in (if (hiveSupported && hiveVarcharSupported) {
        val df2 = spark.sql(
            """
              |SELECT
              |  x.col_0,
              |  x.col_1
              |FROM sql_builder_2 x""".stripMargin)
        val sql2 = new SqlBuilder(df2.queryExecution.analyzed).toSQL
        noException shouldBe thrownBy(spark.sql(sql2))
        sql2 should be("SELECT `col_0`, `col_1` FROM `default`.`sql_builder_2`")
    })
}
