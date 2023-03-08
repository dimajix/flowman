/*
 * Copyright (C) 2022 The Flowman Authors
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

import java.util.Locale

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.LongType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.spi.SchemaCheckExecutor
import com.dimajix.spark.sql.DataFrameUtils
import com.dimajix.spark.sql.ExpressionParser
import com.dimajix.spark.sql.SqlParser


final case class SchemaCheckReference(
    override val parent:Option[Reference]
) extends Reference {
    override def toString: String = {
        parent match {
            case Some(ref) => ref.toString + "/check"
            case None => ""
        }
    }
    override def kind : String = "schema_check"
}


abstract class SchemaCheck extends Fragment with Product with Serializable {
    def name : String
    def text: String = filter match {
        case Some(condition) => s"$name WHERE $condition"
        case None => name
    }
    def filter : Option[String]
    def result : Option[CheckResult]
    def withResult(result:CheckResult) : SchemaCheck

    override def parent: Option[Reference]
    override def reference: SchemaCheckReference = SchemaCheckReference(parent)
    override def fragments: Seq[Fragment] = result.toSeq
    override def reparent(parent: Reference): SchemaCheck
}

final case class PrimaryKeySchemaCheck(
    parent:Option[Reference],
    description:Option[String] = None,
    columns:Seq[String] = Seq.empty,
    result:Option[CheckResult] = None,
    filter:Option[String] = None
) extends SchemaCheck {
    override def name : String = s"PRIMARY KEY (${columns.mkString(",")})"
    override def withResult(result: CheckResult): SchemaCheck = copy(result=Some(result))
    override def reparent(parent: Reference): PrimaryKeySchemaCheck = {
        val ref = SchemaCheckReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

final case class ForeignKeySchemaCheck(
    parent:Option[Reference],
    description: Option[String] = None,
    columns: Seq[String] = Seq.empty,
    relation: Option[RelationIdentifier] = None,
    mapping: Option[MappingOutputIdentifier] = None,
    references: Seq[String] = Seq.empty,
    result:Option[CheckResult] = None,
    filter:Option[String] = None
) extends SchemaCheck {
    override def name : String = {
        val otherEntity = relation.map(_.toString).orElse(mapping.map(_.toString)).getOrElse("")
        val otherColumns = if (references.isEmpty) columns else references
        s"FOREIGN KEY (${columns.mkString(",")}) REFERENCES ${otherEntity}(${otherColumns.mkString(",")})"
    }
    override def withResult(result: CheckResult): SchemaCheck = copy(result=Some(result))
    override def reparent(parent: Reference): ForeignKeySchemaCheck = {
        val ref = SchemaCheckReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

final case class ExpressionSchemaCheck(
    parent:Option[Reference],
    description: Option[String] = None,
    expression: String,
    result:Option[CheckResult] = None,
    filter:Option[String] = None
) extends SchemaCheck {
    override def name: String = expression
    override def withResult(result: CheckResult): SchemaCheck = copy(result=Some(result))
    override def reparent(parent: Reference): ExpressionSchemaCheck = {
        val ref = SchemaCheckReference(Some(parent))
        copy(parent=Some(parent), result=result.map(_.reparent(ref)))
    }
}

final case class SqlSchemaCheck(
    parent: Option[Reference],
    description: Option[String] = None,
    query: String,
    result: Option[CheckResult] = None,
    filter: Option[String] = None
) extends SchemaCheck {
    override def name: String = query
    override def withResult(result: CheckResult): SchemaCheck = copy(result = Some(result))
    override def reparent(parent: Reference): SqlSchemaCheck = {
        val ref = SchemaCheckReference(Some(parent))
        copy(parent = Some(parent), result = result.map(_.reparent(ref)))
    }
}



class DefaultSchemaCheckExecutor extends SchemaCheckExecutor {
    override def execute(execution: Execution, context:Context, df0: DataFrame, check: SchemaCheck): Option[CheckResult] = {
        val df = applyFilter(execution, context, df0, check.filter)
        check match {
            case p:PrimaryKeySchemaCheck =>
                val cols = p.columns.map(df(_))
                val agg = df.filter(cols.map(_.isNotNull).reduce(_ || _)).groupBy(cols:_*).count()
                val result = agg.groupBy(agg(agg.columns(cols.length)) > 1).count().collect()
                val numSuccess = result.find(_.getBoolean(0) == false).map(_.getLong(1)).getOrElse(0L)
                val numFailed = result.find(_.getBoolean(0) == true).map(_.getLong(1)).getOrElse(0L)
                val status = if (numFailed > 0) CheckStatus.FAILED else CheckStatus.SUCCESS
                val description = s"$numSuccess keys are unique, $numFailed keys are non-unique"
                Some(CheckResult(Some(check.reference), status, Some(description)))

            case f:ForeignKeySchemaCheck =>
                val otherDf =
                    f.relation.map { rel =>
                        val relation = context.getRelation(rel)
                        relation.read(execution)
                    }.orElse(f.mapping.map { map=>
                        val mapping = context.getMapping(map.mapping)
                        execution.instantiate(mapping, map.output)
                    }).getOrElse(throw new IllegalArgumentException(s"Need either mapping or relation in foreignKey test ${check.reference.toString}"))
                val thisCols = f.columns.map(df(_))
                val otherCols =
                    if (f.references.nonEmpty)
                        f.references.map(otherDf(_))
                    else
                        f.columns.map(otherDf(_))
                // Remove NULL entries and make data distinct to avoid explosion of join in case of duplicates
                val otherDistinctDf = otherDf
                    .filter(otherCols.map(_.isNotNull).reduce(_ || _))
                    .select(otherCols:_*)
                    .distinct()
                val joined = df.filter(thisCols.map(_.isNotNull).reduce(_ && _))
                    .join(otherDistinctDf, thisCols.zip(otherCols).map(lr => lr._1 === lr._2).reduce(_ && _), "left")
                executePredicateTest(joined, check, otherCols.map(_.isNotNull).reduce(_ || _))

            case e:ExpressionSchemaCheck =>
                executePredicateTest(df, check, coalesce(expr(e.expression).cast(BooleanType), lit(false)))

            case s:SqlSchemaCheck =>
                val deps = SqlParser.resolveDependencies(s.query)
                    .filter(_.toLowerCase(Locale.ROOT) != "__this__")
                    .toSeq
                    .map { dep =>
                        val mapping = context.getMapping(MappingIdentifier(dep))
                        dep -> execution.instantiate(mapping, mapping.output.output)
                    } :+ ("__this__" -> df)

                val df1 = DataFrameUtils.withTempViews(deps) {
                    execution.spark.sql(s.query)
                }

                val plan = df1.queryExecution.analyzed
                plan.maxRows match {
                    case Some(1) if df1.columns.contains("success") =>
                        val cols = df1.columns
                        val result = df1.withColumn("success", df1("success").cast(BooleanType)).first()
                        val success = result.getBoolean(cols.indexOf("success"))
                        val values = cols.zipWithIndex
                            .filter(_._1 != "success")
                            .map { case(col,idx) => col + "=" + result.get(idx).toString }
                        val status = if (success) CheckStatus.SUCCESS else CheckStatus.FAILED
                        val description = values.mkString(", ")
                        Some(CheckResult(Some(s.reference), status, Some(description)))
                    case _ if df1.columns.length == 2 =>
                        val cols = plan.output
                        val boolCol = coalesce(new Column(cols(0)).cast(BooleanType), lit(false)).as("bool_col")
                        val countCol = new Column(cols(1)).cast(LongType).as("count_col")
                        val df2 = df1.select(boolCol, countCol)
                        val result = df2.groupBy(df2("bool_col")).sum("count_col")
                        evaluateResult(result, s)
                    case _ =>
                        throw new IllegalArgumentException("Schema check either needs to return a single row with a column 'success', or it needs to return exactly two columns 'success', 'count' with possibly multiple rows")
                }

            case _ => None
        }
    }

    private def executePredicateTest(df: DataFrame, test:SchemaCheck, predicate:Column) : Option[CheckResult] = {
        val result = df.groupBy(predicate).count()
        evaluateResult(result, test)
    }

    private def evaluateResult(df:DataFrame, test:SchemaCheck) : Option[CheckResult] = {
        val result = df.collect()
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
                    .toSeq
                    .map(d => d ->instantiate(d))
                DataFrameUtils.withTempViews(deps) {
                    df.filter(expr(filter))
                }
            case None => df
        }
    }
}
