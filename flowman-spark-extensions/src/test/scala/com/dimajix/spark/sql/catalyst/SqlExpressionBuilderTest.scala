/*
 * Copyright (C) 2024 The Flowman Authors
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

import com.dimajix.spark.sql.ExpressionParser
import com.dimajix.spark.testing.LocalSparkSession


class SqlExpressionBuilderTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    behavior of "The SqlExpressionBuilder"

    it should "support bitwise expressions" in {
        val expr1 = ExpressionParser.parseExpression("~x")
        val sql1 = SqlExpressionBuilder.toSql(expr1)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql1))
        sql1 should be ("~`x`")

        val expr2 = ExpressionParser.parseExpression("x | y")
        val sql2 = SqlExpressionBuilder.toSql(expr2)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql2))
        sql2 should be ("(`x` | `y`)")

        val expr3 = ExpressionParser.parseExpression("x & y")
        val sql3 = SqlExpressionBuilder.toSql(expr3)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql3))
        sql3 should be ("(`x` & `y`)")
    }

    it should "support logical expressions" in {
        val expr1 = ExpressionParser.parseExpression("NOT x")
        val sql1 = SqlExpressionBuilder.toSql(expr1)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql1))
        sql1 should be ("(NOT `x`)")

        val expr2 = ExpressionParser.parseExpression("x OR y")
        val sql2 = SqlExpressionBuilder.toSql(expr2)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql2))
        sql2 should be ("(`x` OR `y`)")

        val expr3 = ExpressionParser.parseExpression("x AND y")
        val sql3 = SqlExpressionBuilder.toSql(expr3)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql3))
        sql3 should be ("(`x` AND `y`)")
    }

    it should "support comparisons" in {
        val expr1 = ExpressionParser.parseExpression("x > y")
        val sql1 = SqlExpressionBuilder.toSql(expr1)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql1))
        sql1 should be ("(`x` > `y`)")

        val expr2 = ExpressionParser.parseExpression("x IN (1,3,y)")
        val sql2 = SqlExpressionBuilder.toSql(expr2)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql2))
        sql2 should be ("(`x` IN (1, 3, `y`))")

        val expr3 = ExpressionParser.parseExpression("RLIKE('%SystemDrive%\\\\Users\\\\John', '%SystemDrive%\\\\Users.*')")
        val sql3 = SqlExpressionBuilder.toSql(expr3)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql3))
        sql3 should be ("RLIKE('%SystemDrive%\\\\Users\\\\John', '%SystemDrive%\\\\Users.*')")

        val expr4 = ExpressionParser.parseExpression("x LIKE '%John%'")
        val sql4 = SqlExpressionBuilder.toSql(expr4)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql4))
        sql4 should be ("`x` LIKE '%John%'")
    }

    it should "support basic expressions" in {
        val expr1 = ExpressionParser.parseExpression("x IS NOT NULL")
        val sql1 = SqlExpressionBuilder.toSql(expr1)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql1))
        sql1 should be ("`x` IS NOT NULL")

        val expr2 = ExpressionParser.parseExpression("x IS NULL")
        val sql2 = SqlExpressionBuilder.toSql(expr2)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql2))
        sql2 should be ("`x` IS NULL")
    }

    it should "support cast expressions" in {
        val expr1 = ExpressionParser.parseExpression("CAST(x AS DOUBLE)")
        val sql1 = SqlExpressionBuilder.toSql(expr1)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql1))
        sql1 should be ("CAST(`x` AS DOUBLE)")
    }

    it should "support control flow expressions" in {
        val expr1 = ExpressionParser.parseExpression("IF(x >= 12, a, b)")
        val sql1 = SqlExpressionBuilder.toSql(expr1)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql1))
        sql1 should be ("IF((`x` >= 12), `a`, `b`)")

        val expr2 = ExpressionParser.parseExpression("CASE WHEN x >= 22 THEN a WHEN x < -10 THEN b ELSE 23 END AS result")
        val sql2 = SqlExpressionBuilder.toSql(expr2)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql2))
        sql2 should be ("CASE WHEN (`x` >= 22) THEN `a` WHEN (`x` < -10) THEN `b` ELSE 23 END AS `result`")
    }

    it should "support various functions" in {
        val expr1 = ExpressionParser.parseExpression("CONCAT(x, 'abc') AS result")
        val sql1 = SqlExpressionBuilder.toSql(expr1)
        noException shouldBe thrownBy(ExpressionParser.parseExpression(sql1))
        sql1 should be ("CONCAT(`x`, 'abc') AS `result`")
    }
}
