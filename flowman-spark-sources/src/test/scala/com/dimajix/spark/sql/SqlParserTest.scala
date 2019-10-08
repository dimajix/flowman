/*
 * Copyright 2019 Kaya Kupferschmidt
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

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class SqlParserTest extends FlatSpec with Matchers {
    "The SqlParser" should "detect all dependencies" in {
        val deps = SqlParser.resolveDependencies(
            """
              |SELECT * FROM db.lala
              |""".stripMargin)
        deps should be (Seq("db.lala"))
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
        deps should be (Seq("db.lala"))
    }
}
