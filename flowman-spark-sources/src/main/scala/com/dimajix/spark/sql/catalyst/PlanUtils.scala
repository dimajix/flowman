/*
 * Copyright 2020 Kaya Kupferschmidt
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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.With
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType


object PlanUtils {
    def analyze(spark:SparkSession, logicalPlan: LogicalPlan) : LogicalPlan = {
        spark.sessionState.analyzer.execute(logicalPlan)
    }

    def singleRow(spark:SparkSession, schema:StructType) : DataFrame = {
        val logicalPlan = PlanUtils.singleRowPlan(schema)
        new Dataset[Row](spark, logicalPlan, RowEncoder(schema))
    }

    def singleRowPlan(schema:StructType) : LogicalPlan = {
        val expressions = schema.map { field =>
            val literal =
                if (field.nullable) {
                    Literal(null, field.dataType)
                }
                else {
                    field.dataType match {
                        case ByteType =>
                            Literal(0.toByte, ByteType)
                        case ShortType =>
                            Literal(0.toShort, ShortType)
                        case IntegerType=>
                            Literal(0, IntegerType)
                        case LongType =>
                            Literal(0L, LongType)
                        case FloatType =>
                            Literal(0f, FloatType)
                        case BooleanType =>
                            Literal(false, BooleanType)
                        case DoubleType =>
                            Literal(0.0, DoubleType)
                        case dt:DecimalType =>
                            Literal(BigDecimal(0), dt)
                        case StringType=>
                            Literal("", StringType)
                    }
                }
            Alias(literal, field.name)(explicitMetadata = Option(field.metadata))
        }
        Project(expressions, OneRowRelation())
    }

    /**
     * Replace all dependencies in a given LogicalPlan by dummy dependencies with the specified schema
     */
    def replaceDependencies(plan:LogicalPlan, deps:Map[String, StructType]) : LogicalPlan = {
        object ReplaceRelation extends Rule[LogicalPlan] {
            val replacements = deps.map { case (name, schema) =>
                name -> singleRowPlan(schema)
            }

            override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
                case relation: UnresolvedRelation =>
                    replacements.getOrElse(relation.tableName, relation)
                case With(child, ctes) =>
                    With(child, ctes.map { case(k,v) => k -> v.copy(child = apply(v.child)) })
            }
        }

        object Replacer extends RuleExecutor[LogicalPlan] {
            override protected def batches: Seq[Batch] = Seq(
                Batch("Replace all relations", Once, ReplaceRelation)
            )
        }

        Replacer.execute(plan)
    }
}
