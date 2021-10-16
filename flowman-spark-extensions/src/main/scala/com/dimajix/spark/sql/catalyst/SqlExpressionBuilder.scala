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

package com.dimajix.spark.sql.catalyst

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Descending
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Lag
import org.apache.spark.sql.catalyst.expressions.Lead
import org.apache.spark.sql.catalyst.expressions.NullsFirst
import org.apache.spark.sql.catalyst.expressions.NullsLast
import org.apache.spark.sql.catalyst.expressions.RowNumber
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.WindowExpression
import org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition
import org.apache.spark.sql.catalyst.util.quoteIdentifier


object SqlExpressionBuilder {
    def toSql(exprs: Seq[Expression], prefix: String): Seq[String] = {
        Seq(exprs).filter(_.nonEmpty).map(_.map(toSql).mkString(prefix, ", ", ""))
    }

    /**
      * This helper method converts a Spark expression to SQL and catches some corner cases in order to provide
      * better compatibility with Hive and Impala
      */
    def toSql(expr:Expression) : String = {
        expr match {
            case alias @ Alias(expr @ WindowExpression(RowNumber(), _), name) =>
                unboundedWindowExpressionToSql(alias, expr)
            case alias @ Alias(expr @ WindowExpression(_:Lead, _), name) =>
                unboundedWindowExpressionToSql(alias, expr)
            case alias @ Alias(expr @ WindowExpression(_:Lag, _), name) =>
                unboundedWindowExpressionToSql(alias, expr)
            case alias @ Alias(expr:WindowExpression, name) =>
                windowExpressionToSql(alias, expr)
            case order : SortOrder =>
                sortOrderToSsql(order)
            case _ => expr.sql
        }
    }

    def sortOrderToSsql(order:SortOrder) : String = {
        order match {
            // Do not emit NULLS LAST / NULLS FIRST if default is used. This makes SQL compatible with Hive < 2.1.0
            case SortOrder(_, Ascending, NullsFirst, _) => order.child.sql + " " + order.direction.sql
            case SortOrder(_, Descending, NullsLast, _) => order.child.sql + " " + order.direction.sql
            case _ => order.child.sql + " " + order.direction.sql + " " + order.nullOrdering.sql
        }
    }

    private def windowExpressionToSql(alias:Alias, expr:WindowExpression) : String = {
        val wf = expr.windowFunction
        val ws = expr.windowSpec
        val qualifierPrefix = alias.qualifier.map(_ + ".").headOption.getOrElse("")
        s"${wf.sql + " OVER " + windowSpecToSql(ws)} AS $qualifierPrefix${quoteIdentifier(alias.name)}"
    }

    def windowSpecToSql(spec:WindowSpecDefinition) : String = {
        val elements =
            toSql(spec.partitionSpec, "PARTITION BY ") ++
                toSql(spec.orderSpec, "ORDER BY ")++
                Seq(spec.frameSpecification.sql)
        elements.mkString("(", " ", ")")
    }

    private def unboundedWindowExpressionToSql(alias:Alias, expr:WindowExpression) : String = {
        val wf = expr.windowFunction
        val ws = expr.windowSpec
        val qualifierPrefix = alias.qualifier.map(_ + ".").headOption.getOrElse("")
        s"${wf.sql + " OVER " + unboundedWindowSpecToSQL(ws)} AS $qualifierPrefix${quoteIdentifier(alias.name)}"
    }

    private def unboundedWindowSpecToSQL(spec:WindowSpecDefinition) : String = {
        val elements =
            toSql(spec.partitionSpec, "PARTITION BY ") ++
                toSql(spec.orderSpec, "ORDER BY ")
        elements.mkString("(", " ", ")")
    }

}
