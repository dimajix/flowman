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
    "The SQLBuilder" should "support basic selects" in {
        spark.sql(
            """
              CREATE TABLE sql_builder_0(
                col_0 INT,
                col_1 STRING
              )
            """)

        val df1 = spark.sql("SELECT * FROM sql_builder_0")
        val sql1 = new SQLBuilder(df1.queryExecution.analyzed).toSQL
        println(sql1)

        val df2 = spark.sql("SELECT CONCAT(col_0, col_1) AS result FROM sql_builder_0")
        val sql2 = new SQLBuilder(df2.queryExecution.analyzed).toSQL
        println(sql2)
    }
}
