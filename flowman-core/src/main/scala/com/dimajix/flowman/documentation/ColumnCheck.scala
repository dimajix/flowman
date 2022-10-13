/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.documentation

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.types.BooleanType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.spi.ColumnCheckExecutor
import com.dimajix.spark.sql.DataFrameUtils
import com.dimajix.spark.sql.ExpressionParser


final case class ColumnCheckReference(
    override val parent:Option[Reference]
) extends Reference {
    override def toString: String = {
        parent match {
            case Some(ref) => ref.toString + "/check"
            case None => ""
        }
    }
    override def kind : String = "column_check"
}


abstract class ColumnCheck extends Fragment with Product with Serializable {
    def name : String
    def text : String = filter match {
        case Some(condition) => s"$name WHERE $condition"
        case None => name
    }
    def filter : Option[String]
    def result : Option[CheckResult]
    def withResult(result:CheckResult) : ColumnCheck

    override def reparent(parent: Reference): ColumnCheck

    override def parent: Option[Reference]
    override def reference: ColumnCheckReference = ColumnCheckReference(parent)
    override def fragments: Seq[Fragment] = result.toSeq
}


final case class NotNullColumnCheck(
    parent:Option[Reference],
    description: Option[String] = None,
    result:Option[CheckResult] = None,
    filter:Option[String] = None
) extends ColumnCheck {
    override def name : String = "IS NOT NULL"
    override def withResult(result: CheckResult): ColumnCheck = copy(result=Some(result))
    override def reparent(parent: Reference): ColumnCheck = {
        val ref = ColumnCheckReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

final case class UniqueColumnCheck(
    parent:Option[Reference],
    description: Option[String] = None,
    result:Option[CheckResult] = None,
    filter:Option[String] = None
) extends ColumnCheck  {
    override def name : String = "HAS UNIQUE VALUES"
    override def withResult(result: CheckResult): ColumnCheck = copy(result=Some(result))
    override def reparent(parent: Reference): UniqueColumnCheck = {
        val ref = ColumnCheckReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

final case class RangeColumnCheck(
    parent:Option[Reference],
    description: Option[String] = None,
    lower:Any,
    upper:Any,
    result:Option[CheckResult] = None,
    filter:Option[String] = None
) extends ColumnCheck  {
    override def name : String = s"IS BETWEEN $lower AND $upper"
    override def withResult(result: CheckResult): ColumnCheck = copy(result=Some(result))
    override def reparent(parent: Reference): RangeColumnCheck = {
        val ref = ColumnCheckReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

final case class ValuesColumnCheck(
    parent:Option[Reference],
    description: Option[String] = None,
    values: Seq[Any] = Seq(),
    result:Option[CheckResult] = None,
    filter:Option[String] = None
) extends ColumnCheck  {
    override def name : String = s"IS IN (${values.mkString(",")})"
    override def withResult(result: CheckResult): ColumnCheck = copy(result=Some(result))
    override def reparent(parent: Reference): ValuesColumnCheck = {
        val ref = ColumnCheckReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

final case class ForeignKeyColumnCheck(
    parent:Option[Reference],
    description: Option[String] = None,
    relation: Option[RelationIdentifier] = None,
    mapping: Option[MappingOutputIdentifier] = None,
    column: Option[String] = None,
    result:Option[CheckResult] = None,
    filter:Option[String] = None
) extends ColumnCheck {
    override def name : String = {
        val otherEntity = relation.map(_.toString).orElse(mapping.map(_.toString)).getOrElse("")
        val otherColumn = column.getOrElse("")
        s"FOREIGN KEY REFERENCES ${otherEntity} (${otherColumn})"
    }
    override def withResult(result: CheckResult): ColumnCheck = copy(result=Some(result))
    override def reparent(parent: Reference): ForeignKeyColumnCheck = {
        val ref = ColumnCheckReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

final case class ExpressionColumnCheck(
    parent:Option[Reference],
    description: Option[String] = None,
    expression: String,
    result:Option[CheckResult] = None,
    filter:Option[String] = None
) extends ColumnCheck {
    override def name: String = expression
    override def withResult(result: CheckResult): ColumnCheck = copy(result=Some(result))
    override def reparent(parent: Reference): ExpressionColumnCheck = {
        val ref = ColumnCheckReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

final case class LengthColumnCheck(
    parent:Option[Reference],
    description: Option[String] = None,
    minimumLength: Option[Int] = None,
    maximumLength: Option[Int] = None,
    result:Option[CheckResult] = None,
    filter:Option[String] = None
) extends ColumnCheck {
    override def name: String = {
        (minimumLength, maximumLength) match {
            case (Some(min),Some(max)) if min != max => s"LENGTH BETWEEN $min AND $max"
            case (Some(min),Some(max)) => s"LENGTH = $min"
            case (None,Some(max)) => s"LENGTH <= $max"
            case (Some(min),None) => s"LENGTH >= $min"
            case _ => s"LENGTH"
        }
    }
    override def withResult(result: CheckResult): ColumnCheck = copy(result=Some(result))
    override def reparent(parent: Reference): LengthColumnCheck = {
        val ref = ColumnCheckReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}


class DefaultColumnCheckExecutor extends ColumnCheckExecutor {
    override def execute(execution: Execution, context:Context, df0: DataFrame, column:String, check: ColumnCheck): Option[CheckResult] = {
        val df = applyFilter(execution, context, df0, check.filter)
        check match {
            case _: NotNullColumnCheck =>
                executePredicateTest(df, check, df(column).isNotNull)

            case _: UniqueColumnCheck =>
                val agg = df.filter(df(column).isNotNull).groupBy(df(column)).count()
                val result = agg.groupBy(agg(agg.columns(1)) > 1).count().collect()
                val numSuccess = result.find(_.getBoolean(0) == false).map(_.getLong(1)).getOrElse(0L)
                val numFailed = result.find(_.getBoolean(0) == true).map(_.getLong(1)).getOrElse(0L)
                val status = if (numFailed > 0) CheckStatus.FAILED else CheckStatus.SUCCESS
                val description = s"$numSuccess values are unique, $numFailed values are non-unique"
                Some(CheckResult(Some(check.reference), status, Some(description)))

            case v: ValuesColumnCheck =>
                val dt = df.schema(column).dataType
                val values = v.values.map(v => lit(v).cast(dt))
                executePredicateTest(df.filter(df(column).isNotNull), check, df(column).isin(values:_*))

            case v: RangeColumnCheck =>
                val dt = df.schema(column).dataType
                val lower = lit(v.lower).cast(dt)
                val upper = lit(v.upper).cast(dt)
                executePredicateTest(df.filter(df(column).isNotNull), check, df(column).between(lower, upper))

            case v: ExpressionColumnCheck =>
                executePredicateTest(df, check, coalesce(expr(v.expression).cast(BooleanType), lit(false)))

            case l: LengthColumnCheck =>
                val condition =  (l.minimumLength, l.maximumLength) match {
                    case (Some(min),Some(max)) if min != max => length(df(column)).between(min,max)
                    case (Some(min),Some(max)) => length(df(column)) === min
                    case (None,Some(max)) => length(df(column)) <= max
                    case (Some(min),None) => length(df(column)) >= min
                    case _ => lit(true)
                }
                executePredicateTest(df.filter(df(column).isNotNull), check, condition)

            case f:ForeignKeyColumnCheck =>
                val otherDf =
                    f.relation.map { rel =>
                        val relation = context.getRelation(rel)
                        relation.read(execution)
                    }.orElse(f.mapping.map { map=>
                        val mapping = context.getMapping(map.mapping)
                        execution.instantiate(mapping, map.output)
                    }).getOrElse(throw new IllegalArgumentException(s"Need either mapping or relation in foreignKey test of column '$column' in check ${check.reference.toString}"))
                val thisCol = df(column)
                val otherCol = otherDf(f.column.getOrElse(column))
                // Remove NULL entries and make data distinct to avoid explosion of join in case of duplicates
                val otherDistinctDf = otherDf
                    .filter(otherCol.isNotNull)
                    .select(otherCol)
                    .distinct()
                val joined = df
                    .filter(thisCol.isNotNull)
                    .join(otherDistinctDf, thisCol === otherCol, "left")
                executePredicateTest(joined, check, otherCol.isNotNull)

            case _ => None
        }
    }

    private def executePredicateTest(df: DataFrame, test:ColumnCheck, predicate:Column) : Option[CheckResult] = {
        val result = df.groupBy(predicate).count().collect()
        val numSuccess = result.find(r => r.getBoolean(0)).map(_.getLong(1)).getOrElse(0L)
        val numFailed = result.find(r => !r.getBoolean(0)).map(_.getLong(1)).getOrElse(0L)
        val status = if (numFailed > 0) CheckStatus.FAILED else CheckStatus.SUCCESS
        val description = s"$numSuccess records passed, $numFailed records failed"
        Some(CheckResult(Some(test.reference), status, Some(description)))
    }

    private def applyFilter(execution: Execution, context:Context, df:DataFrame, filter:Option[String]) : DataFrame = {
        def instantiate(dep:String) : DataFrame = {
            val mapping = context.getMapping(MappingIdentifier(dep))
            execution.instantiate(mapping, mapping.output.output)
        }
        filter match {
            case Some(filter) =>
                val deps = ExpressionParser.resolveDependencies(filter)
                    .map(d => d ->instantiate(d))
                DataFrameUtils.withTempViews(deps) {
                    df.filter(expr(filter))
                }
            case None => df
        }
    }
}
