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
  behavior of "The SQLBuilder"

  it should "support basic selects" in (if (hiveSupported) {
    spark.sql(
      """
              CREATE TABLE sql_builder_0(
                col_0 INT,
                col_1 STRING
              )
            """)

    val df1 = spark.sql("SELECT * FROM sql_builder_0")
    val sql1 = new SQLBuilder(df1.queryExecution.analyzed).toSQL
    noException shouldBe thrownBy(spark.sql(sql1))
    println(sql1)

    val df2 = spark.sql("SELECT CONCAT(col_0, col_1) AS result FROM sql_builder_0")
    val sql2 = new SQLBuilder(df2.queryExecution.analyzed).toSQL
    noException shouldBe thrownBy(spark.sql(sql2))
    println(sql2)

    val df3 = spark.sql("SELECT CONCAT(x.col_0, x.col_1) AS result FROM sql_builder_0 AS x")
    val sql3 = new SQLBuilder(df3.queryExecution.analyzed).toSQL
    noException shouldBe thrownBy(spark.sql(sql3))
    println(sql3)

    val df4 = spark.sql("SELECT CONCAT(x.col_0, x.col_1) AS result FROM sql_builder_0 AS x WHERE x.col_0 = 67")
    val sql4 = new SQLBuilder(df4.queryExecution.analyzed).toSQL
    noException shouldBe thrownBy(spark.sql(sql4))
    println(sql4)

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
    println(sql5)

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
    println(sql6)
  })
}
