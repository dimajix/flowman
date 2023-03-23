/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class SqlExpressionTest extends AnyFlatSpec with Matchers {
    "The SqlParser" should "parse simple columns" in {
        val parser = CatalystSqlParser
        val result = parser.parseExpression("some_column")
        result.references.map(_.name).toSeq.contains("some_column") should be (true)
    }
    it should "parse complex expressions" in {
        val parser = CatalystSqlParser
        val result = parser.parseExpression("case when some_column > 0 then some_udf(other_column) else max(third_column) end")
        result.references.map(_.name).toSeq.contains("some_column") should be (true)
        result.references.map(_.name).toSeq.contains("other_column") should be (true)
        result.references.map(_.name).toSeq.contains("third_column") should be (true)
    }
}
