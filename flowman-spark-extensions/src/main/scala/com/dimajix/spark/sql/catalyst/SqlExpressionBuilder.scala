/*
 * Copyright (C) 2019 The Flowman Authors
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

import java.util.Locale

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.BinaryOperator
import org.apache.spark.sql.catalyst.expressions.BitwiseNot
import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Descending
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.In
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.apache.spark.sql.catalyst.expressions.Lag
import org.apache.spark.sql.catalyst.expressions.Lead
import org.apache.spark.sql.catalyst.expressions.Like
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.NullsFirst
import org.apache.spark.sql.catalyst.expressions.NullsLast
import org.apache.spark.sql.catalyst.expressions.RowNumber
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.UnaryMinus
import org.apache.spark.sql.catalyst.expressions.WindowExpression
import org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.StructType


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
            case cast:Cast =>
                castToSQL(cast)
            case alias:Alias =>
                aliasToSQL(alias)
            case ref:AttributeReference  =>
                attributeReferenceToSQL(ref)
            case agg:AggregateExpression =>
                aggregateToSQL(agg)
            case expr:BinaryOperator =>
                s"(${toSql(expr.left)} ${expr.sqlOperator} ${toSql(expr.right)})"
            case expr:UnaryMinus =>
                s"(- ${toSql(expr.child)})"
            case expr:Not =>
                s"(NOT ${toSql(expr.child)})"
            case expr:BitwiseNot =>
                s"~${toSql(expr.child)}"
            case expr:IsNull =>
                s"${toSql(expr.child)} IS NULL"
            case expr:IsNotNull =>
                s"${toSql(expr.child)} IS NOT NULL"
            case expr:CaseWhen =>
                s"CASE" + expr.branches.map(b =>
                    " WHEN " + toSql(b._1) + " THEN " + toSql(b._2)
                ).mkString +
                    expr.elseValue.map(e => " ELSE " + toSql(e)).getOrElse("") +
                " END"
            case in:In =>
                s"(${toSql(in.value)} IN (${in.list.map(toSql).mkString(", ")}))"
            case like:Like =>
                s"${toSql(like.left)} LIKE ${toSql(like.right)}"
            case order : SortOrder =>
                sortOrderToSsql(order)
            case l: Literal =>
                l.sql
            case a: UnresolvedAttribute =>
                a.nameParts.map(quoteIdentifier).mkString(".")
            case _ =>
                s"${expr.prettyName}(${expr.children.map(toSql).mkString(", ")})"
        }
    }

    def sortOrderToSsql(order:SortOrder) : String = {
        order match {
            // Do not emit NULLS LAST / NULLS FIRST if default is used. This makes SQL compatible with Hive < 2.1.0
            case SortOrder(_, Ascending, NullsFirst, _) => toSql(order.child) + " " + order.direction.sql
            case SortOrder(_, Descending, NullsLast, _) => toSql(order.child) + " " + order.direction.sql
            case _ => toSql(order.child) + " " + order.direction.sql + " " + order.nullOrdering.sql
        }
    }

    private def windowExpressionToSql(alias:Alias, expr:WindowExpression) : String = {
        val wf = expr.windowFunction
        val ws = expr.windowSpec
        val qualifierPrefix = alias.qualifier.map(_ + ".").headOption.getOrElse("")
        s"${toSql(wf) + " OVER " + windowSpecToSql(ws)} AS $qualifierPrefix${quoteIdentifier(alias.name)}"
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
        s"${toSql(wf) + " OVER " + unboundedWindowSpecToSQL(ws)} AS $qualifierPrefix${quoteIdentifier(alias.name)}"
    }

    private def unboundedWindowSpecToSQL(spec:WindowSpecDefinition) : String = {
        val elements =
            toSql(spec.partitionSpec, "PARTITION BY ") ++
                toSql(spec.orderSpec, "ORDER BY ")
        elements.mkString("(", " ", ")")
    }

    private def castToSQL(cast:Cast): String = {
        cast.dataType match {
            // HiveQL doesn't allow casting to complex types. For logical plans translated from HiveQL, this
            // type of casting can only be introduced by the analyzer, and can be omitted when converting
            // back to SQL query string.
            case _: ArrayType | _: MapType | _: StructType => toSql(cast.child)
            case _ => s"${cast.prettyName.toUpperCase(Locale.ROOT)}(${toSql(cast.child)} AS ${cast.dataType.sql})"
        }
    }

    private def aliasToSQL(alias:Alias): String = {
        val qualifierPrefix =
            if (alias.qualifier.nonEmpty) alias.qualifier.map(quoteIdentifier).mkString(".") + "." else ""
        s"${toSql(alias.child)} AS $qualifierPrefix${quoteIdentifier(alias.name)}"
    }

    private def attributeReferenceToSQL(expr:AttributeReference): String = {
        val qualifierPrefix =
            if (expr.qualifier.nonEmpty) expr.qualifier.map(quoteIdentifier).mkString(".") + "." else ""
        s"$qualifierPrefix${quoteIdentifier(expr.name)}"
    }

    private def aggregateToSQL(agg: AggregateExpression) : String = {
        val func = agg.aggregateFunction
        val distinct = if (agg.isDistinct) "DISTINCT " else ""
        val aggFuncStr = s"${func.prettyName}($distinct${func.children.map(toSql).mkString(", ")})"
        agg.filter match {
            case Some(predicate) => s"$aggFuncStr FILTER (WHERE ${toSql(predicate)})"
            case _ => aggFuncStr
        }
    }
}
