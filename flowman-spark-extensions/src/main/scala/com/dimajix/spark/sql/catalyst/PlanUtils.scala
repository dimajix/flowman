/*
 * Copyright (C) 2020 The Flowman Authors
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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.Unevaluable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.With
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructType


case class NamedAttribute(name:String, dataType: DataType)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifier: Seq[String] = Seq.empty,
    val explicitMetadata: Metadata = Metadata.empty
)
extends Attribute with Unevaluable {
    @transient
    override lazy val references: AttributeSet = AttributeSet.empty

    override def newInstance(): NamedAttribute = this

    override def nullable: Boolean = true

    override def metadata: Metadata = explicitMetadata

    override def withNullability(newNullability: Boolean): Attribute = this
    override def withQualifier(newQualifier: Seq[String]): Attribute = NamedAttribute(name, dataType)(exprId, newQualifier, explicitMetadata)
    override def withName(newName: String): Attribute = NamedAttribute(newName, dataType)(exprId, qualifier, explicitMetadata)
    override def withMetadata(newMetadata: Metadata): Attribute = NamedAttribute(name, dataType)(exprId, qualifier, newMetadata)
    /*override*/ def withExprId(newExprId: ExprId): Attribute = NamedAttribute(name, dataType)(newExprId, qualifier, metadata)
    /*override*/ def withDataType(newType: DataType): Attribute = NamedAttribute(name, newType)(exprId, qualifier, metadata)
}


object PlanUtils {
    def analyze(spark:SparkSession, logicalPlan: LogicalPlan) : LogicalPlan = {
        spark.sessionState.analyzer.execute(logicalPlan)
    }

    def singleRowPlan(schema:StructType) : LogicalPlan = {
        val expressions = schema.map { field =>
            val literal =
                if (field.nullable) {
                    Literal(null, field.dataType)
                }
                else {
                    Literal.default(field.dataType)
                }
            Alias(literal, field.name)(explicitMetadata = Option(field.metadata))
        }
        Project(expressions, OneRowRelation())
    }

    def namedAttributePlan(schema:StructType) : LogicalPlan = {
        val expressions = schema.map { field =>
            NamedAttribute(field.name, field.dataType)(explicitMetadata = field.metadata)
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
