/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.mapping

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.GroupingSets
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.grouping_id

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.mapping.GroupedAggregateMappingSpec.GroupSpec
import com.dimajix.spark.sql.DataFrameUtils


object GroupedAggregateMapping {
    case class Group(
        name:String,
        dimensions:Seq[String],
        aggregations:Seq[String]
    )
}

case class GroupedAggregateMapping(
    instanceProperties : Mapping.Properties,
    input : MappingOutputIdentifier,
    groups : Seq[GroupedAggregateMapping.Group],
    aggregations : Map[String,String],
    partitions : Int = 0
) extends BaseMapping {
    /**
     * Creates an output identifier for the primary output. Since the [[GroupedAggregateMapping]] doesn't have a
     * primary output, it will simply pick the first group - or `cache` when no groups are defined.
     * @return
     */
    override def output: MappingOutputIdentifier = {
        MappingOutputIdentifier(identifier, groups.headOption.map(_.name).getOrElse("cache"))
    }

    /**
     * Lists all outputs of this mapping. Every mapping should have one "main" output, which is the default output
     * implicitly used when no output is specified. But eventually, the "main" output is not mandatory, but
     * recommended.
     * @return
     */
    override def outputs: Seq[String] = groups.map(_.name) :+ "cache"

    /**
     * Returns the dependencies of this mapping, which is exactly one input table
     *
     * @return
     */
    override def inputs : Seq[MappingOutputIdentifier] = {
        Seq(input)
    }

    /**
     * Executes this Mapping and returns a corresponding map of DataFrames per output. The map should contain
     * one entry for each declared output in [[outputs]]. If it contains an additional entry called `cache`, then
     * this [[DataFrame]] will be cached instead of all outputs. The `cache` DataFrame may even well be some
     * internal [[DataFrame]] which is not listed in [[outputs]].
     *
     * @param execution
     * @param input
     * @return
     */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = {
        val groupings = groups.zipWithIndex.map { case (g,i) =>
            struct(g.dimensions.map(d => col(d)):_*).as(s"_flowman_grouping_set_$i")
        }

        val aggregates = aggregations.toSeq.map(kv => expr(kv._2).as(kv._1))
        val expressions = (
            aggregates ++
                groupings :+
                grouping_id().as("_flowman_grouping_id")
            )
            .map(_.expr.asInstanceOf[NamedExpression])

        val df =
            if (partitions > 0)
                input(this.input).repartition(partitions, groupings:_*)
            else
                input(this.input)

        val allGroupings = DataFrameUtils.ofRows(execution.spark, GroupingSets(
            groupings.map(g => Seq(g.expr)),
            groupings.map(_.expr),
            df.queryExecution.logical, expressions))

        val numGroups = groups.size
        val groupingMask = (1 << numGroups) - 1

        val results = groups.zipWithIndex.map { case (group, index) =>
            val groupPrefix = s"_flowman_grouping_set_$index"
            val dimensions = group.dimensions.map(d => col(groupPrefix + "." + d))
            val aggregates = {
                if (group.aggregations.nonEmpty)
                    group.aggregations.map(a => col(a))
                else
                    aggregations.keys.map(a => col(a)).toSeq
            }
            val mask = groupingMask & ~(1 << (numGroups - 1 - index))
            group.name -> allGroupings
                .filter(col("_flowman_grouping_id") === mask)
                .select((dimensions ++ aggregates):_*)
        }

        results.toMap ++ Map("cache" -> allGroupings)
    }
}


object GroupedAggregateMappingSpec {
    class GroupSpec {
        @JsonProperty(value = "name", required = true) private var name: String = _
        @JsonProperty(value = "dimensions", required = true) private var dimensions: Seq[String] = Seq()
        @JsonProperty(value = "aggregations", required = true) private var aggregations: Seq[String] = Seq()

        def instantiate(context: Context) : GroupedAggregateMapping.Group = {
            GroupedAggregateMapping.Group(
                context.evaluate(name),
                dimensions.map(context.evaluate),
                aggregations.map(context.evaluate)
            )
        }
    }
}
class GroupedAggregateMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "groups", required = true) private var groups:Seq[GroupSpec] = Seq()
    @JsonProperty(value = "aggregations", required = true) private[spec] var aggregations: Map[String, String] = _
    @JsonProperty(value = "partitions", required = false) private[spec] var partitions: String = ""

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context): GroupedAggregateMapping = {
        GroupedAggregateMapping(
            instanceProperties(context),
            MappingOutputIdentifier(context.evaluate(input)),
            groups.map(_.instantiate(context)),
            aggregations.map(kv => kv._1 -> context.evaluate(kv._2)),
            if (partitions.isEmpty) 0 else context.evaluate(partitions).toInt
        )
    }
}
