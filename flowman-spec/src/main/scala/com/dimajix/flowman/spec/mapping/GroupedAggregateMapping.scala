/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.grouping_id
import org.apache.spark.sql.functions.struct

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.mapping.GroupedAggregateMappingSpec.GroupSpec
import com.dimajix.spark.sql.DataFrameBuilder
import com.dimajix.spark.sql.DataFrameUtils


object GroupedAggregateMapping {
    case class Group(
        dimensions:Seq[String],
        aggregations:Seq[String],
        filter:Option[String] = None,
        having:Option[String] = None
    )
}

case class GroupedAggregateMapping(
    instanceProperties : Mapping.Properties,
    input : MappingOutputIdentifier,
    groups : Map[String,GroupedAggregateMapping.Group],
    aggregations : Map[String,String],
    partitions : Int = 0
) extends BaseMapping {
    /**
     * Creates an output identifier for the primary output. Since the [[GroupedAggregateMapping]] doesn't have a
     * primary output, it will simply pick the first group - or `cache` when no groups are defined.
     * @return
     */
    override def output: MappingOutputIdentifier = {
        MappingOutputIdentifier(identifier, groups.keys.headOption.getOrElse("cache"))
    }

    /**
     * Lists all outputs of this mapping. Every mapping should have one "main" output, which is the default output
     * implicitly used when no output is specified. But eventually, the "main" output is not mandatory, but
     * recommended.
     * @return
     */
    override def outputs: Set[String] = groups.keys.toSet + "cache"

    /**
     * Returns the dependencies of this mapping, which is exactly one input table
     *
     * @return
     */
    override def inputs : Set[MappingOutputIdentifier] = {
        Set(input)
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
        val df = input(this.input)
        val dimensions = groups.values.flatMap(_.dimensions).toSeq.distinct
        val filters = groups.values.flatMap(_.filter).toSeq.distinct
        if (dimensions.size + filters.size > 31)
            executeViaStructs(df)
        else
            executeDirectly(df)
    }

    private def executeDirectly(input: DataFrame): Map[String, DataFrame] = {
        // Add columns for all filter expressions
        val filters = groups.values.flatMap(_.filter).toSeq.distinct
        val filterIndices = filters.zipWithIndex.toMap
        val filterNames = filters.indices.map(i => s"_flowman_grouping_filter_$i")
        val filteredInput = filters.zip(filterNames).foldLeft(input)((df, fi) => df.withColumn(fi._2, expr(fi._1)))

        // Create GROUP BY dimensions and GROUPING SETs
        val dimensions = groups.values.flatMap(_.dimensions).toSeq.distinct ++ filterNames
        val dimensionIndices = dimensions.zipWithIndex.toMap
        val dimensionColumns = dimensions.map(col)
        val groupingColumns = groups.values.map(g =>
            g.dimensions.map(d => dimensionColumns(dimensionIndices(d))) ++
                g.filter.map(f => col(filterNames(filterIndices(f))))
        ).toSeq

        val allGroupings = performGroupedAggregation(filteredInput, dimensionColumns, groupingColumns)

        // This is a workaround for newer Spark version, which apparently use a different mechanism to derive
        // grouping-ids than Spark up until 3.1.x. It was changed back in Spark 3.3.1
        val legacyMode = input.sparkSession.conf.getOption("spark.sql.legacy.groupingIdWithAppendedUserGroupBy")
            .map(_.toBoolean)
            .getOrElse(org.apache.spark.SPARK_VERSION >= "3.2" && org.apache.spark.SPARK_VERSION < "3.3.1")
        val dimensionIndices2 = {
            if (legacyMode)
                groups.values.flatMap(g => g.dimensions ++ g.filter.map(f => filterNames(filterIndices(f)))).toSeq.distinct.zipWithIndex.toMap
            else
                dimensionIndices
        }
        // Calculate grouping IDs used for extracting individual groups
        val numDimensions = dimensions.size
        val groupingMask = (1 << numDimensions) - 1
        val groupIds = groups.values.map { group =>
            val dimensions = group.dimensions
            val filter = group.filter.map(f => filterNames(filterIndices(f)))
            (groupingMask +: (dimensions ++ filter).map(d => ~(1 << (numDimensions - 1 - dimensionIndices2(d))))).reduce(_ & _)
        }

        // Apply all grouping filters to reduce cache size.
        val cache = if (filterIndices.nonEmpty) {
            val filter = groups.values.zip(groupIds).map { case (group, groupId) =>
                val filter = group.filter.map(f => filterNames(filterIndices(f)))
                createGroupFilter(groupId, filter)
            }.reduce(_ || _)
            allGroupings.filter(filter)
                .drop(filterNames:_*)
        }
        else {
            allGroupings
        }

        val results = groups.zip(groupIds).map { case ((name,group),mask) =>
            val dimensions = group.dimensions
            name -> extractGroup(cache, group, dimensions, mask)
        }

        results ++ Map("cache" -> cache)
    }

    private def executeViaStructs(input: DataFrame): Map[String, DataFrame] = {
        // Add columns for all filter expressions
        val filters = groups.values.flatMap(_.filter).toSeq.distinct
        val filterIndices = filters.zipWithIndex.toMap
        val filterNames = filters.indices.map(i => s"_flowman_grouping_filter_$i")
        val filteredInput = filters.zip(filterNames).foldLeft(input)((df, fi) => df.withColumn(fi._2, expr(fi._1)))

        // Create GROUP BY dimensions and GROUPING SETs via nested structs
        val dimensionColumns = groups.values.zipWithIndex.map { case (g,i) =>
            val filter = g.filter.map(f => filterNames(filterIndices(f)))
            val dimensions = g.dimensions.distinct ++ filter
            struct(dimensions.map(d => col(d)):_*).as(s"_flowman_grouping_set_$i")
        }.toSeq
        val groupingColumns = dimensionColumns.map(g => Seq(g))

        val allGroupings = performGroupedAggregation(filteredInput, dimensionColumns, groupingColumns)

        val numGroups = groups.size
        val groupingMask = (1 << numGroups) - 1

        // Apply all grouping filters to reduce cache size
        val cache = if (filterIndices.nonEmpty) {
            val filter = groups.values.zipWithIndex.map { case (group, index) =>
                val groupPrefix = s"_flowman_grouping_set_$index"
                val filter = group.filter.map(f => groupPrefix + "." + filterNames(filterIndices(f)))
                val groupId = groupingMask & ~(1 << (numGroups - 1 - index))
                createGroupFilter(groupId, filter)
            }.reduce(_ || _)
            allGroupings.filter(filter)
                .drop(filterNames:_*)
        }
        else {
            allGroupings
        }

        // Extract different groupings via filtering
        val results = groups.zipWithIndex.map { case ((name,group), index) =>
            val groupPrefix = s"_flowman_grouping_set_$index"
            val dimensions = group.dimensions.map(d => groupPrefix + "." + d)
            val mask = groupingMask & ~(1 << (numGroups - 1 - index))
            name -> extractGroup(cache, group, dimensions,  mask)
        }

        results ++ Map("cache" -> cache)
    }

    private def createGroupFilter(groupId:Int, filter:Option[String]) : Column = {
        val groupingFilter = (col("_flowman_grouping_id") === groupId)
        filter.map(f => col(f) && groupingFilter).getOrElse(groupingFilter)
    }

    private def extractGroup(allGroupings:DataFrame, group:GroupedAggregateMapping.Group, dimensions:Seq[String], mask:Int) : DataFrame = {
        val aggregates = {
            if (group.aggregations.nonEmpty)
                group.aggregations.map(col)
            else
                aggregations.keys.map(col).toSeq
        }
        val df = allGroupings.filter(col("_flowman_grouping_id") === mask)
            .select((dimensions.map(col) ++ aggregates):_*)

        group.having.map(f => df.filter(f)).getOrElse(df)
    }

    private def performGroupedAggregation(input:DataFrame, dimensions:Seq[Column], groupings:Seq[Seq[Column]]) : DataFrame = {
        val aggregates = aggregations.toSeq.map(kv => expr(kv._2).as(kv._1))
        val expressions = (
            aggregates ++
                dimensions :+
                grouping_id().as("_flowman_grouping_id")
            )
            .map(_.expr.asInstanceOf[NamedExpression])

        val df =
            if (partitions > 0)
                input.repartition(partitions, dimensions:_*)
            else
                input

        DataFrameBuilder.ofRows(input.sparkSession,
            SparkShim.groupingSetAggregate(
                dimensions.map(_.expr),
                groupings.map(g => g.map(_.expr)),
                expressions,
                df.queryExecution.logical
            )
        )
    }
}


object GroupedAggregateMappingSpec {
    class GroupSpec {
        @JsonProperty(value = "dimensions", required = true) private var dimensions: Seq[String] = Seq()
        @JsonProperty(value = "aggregations", required = true) private var aggregations: Seq[String] = Seq()
        @JsonProperty(value = "filter", required = false) private var filter: Option[String] = None
        @JsonProperty(value = "having", required = false) private var having: Option[String] = None

        def instantiate(context: Context) : GroupedAggregateMapping.Group = {
            GroupedAggregateMapping.Group(
                dimensions.map(context.evaluate),
                aggregations.map(context.evaluate),
                context.evaluate(filter),
                context.evaluate(having)
            )
        }
    }
}
class GroupedAggregateMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "groups", required = true) private var groups:Map[String,GroupSpec] = Map()
    @JsonProperty(value = "aggregations", required = true) private[spec] var aggregations: Map[String, String] = _
    @JsonProperty(value = "partitions", required = false) private[spec] var partitions: String = ""

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): GroupedAggregateMapping = {
        GroupedAggregateMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier(context.evaluate(input)),
            groups.map(kv => kv._1 -> kv._2.instantiate(context)),
            aggregations.map(kv => kv._1 -> context.evaluate(kv._2)),
            if (partitions.isEmpty) 0 else context.evaluate(partitions).toInt
        )
    }
}
