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

package com.dimajix.flowman.spec.mapping

import scala.collection.immutable.ListMap

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

import com.dimajix.jackson.ListMapDeserializer

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.spark.sql.ExpressionParser


case class AggregateMapping(
    instanceProperties : Mapping.Properties,
    input : MappingOutputIdentifier,
    dimensions : Seq[String],
    aggregations : Map[String,String],
    filter:Option[String] = None,
    partitions : Int = 0
) extends BaseMapping {
    /**
      * Creates an instance of the aggregated table.
      *
      * @param execution
      * @param tables
      * @return
      */
    override def execute(execution:Execution, tables:Map[MappingOutputIdentifier,DataFrame]): Map[String,DataFrame] = {
        val df = tables(input)
        val dims = dimensions.map(expr)
        val expressions = aggregations.map(kv => expr(kv._2).as(kv._1))

        val aggs = if (partitions > 0)
            df.repartition(partitions, dims:_*).groupBy(dims:_*).agg(expressions.head, expressions.tail.toSeq:_*)
        else
            df.groupBy(dims:_*).agg(expressions.head, expressions.tail.toSeq:_*)

        // Apply optional 'HAVING' filter
        val result = applyFilter(aggs, filter, tables)

        Map("main" -> result)
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @return
      */
    override def inputs : Set[MappingOutputIdentifier] = {
        Set(input) ++ expressionDependencies(filter)
    }
}


class AggregateMappingSpec extends MappingSpec {
    @JsonPropertyDescription("Reference to input mapping")
    @JsonProperty(value = "input", required = true) private[spec] var input: String = _
    @JsonProperty(value = "dimensions", required = true) private[spec] var dimensions: Array[String] = _
    @JsonDeserialize(using = classOf[ListMapDeserializer]) // Old Jackson in old Spark doesn't support ListMap
    @JsonProperty(value = "aggregations", required = true) private[spec] var aggregations: ListMap[String, String] = ListMap()
    @JsonProperty(value = "filter", required = false) private var filter: Option[String] = None
    @JsonProperty(value = "partitions", required = false) private[spec] var partitions: String = ""

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context:Context, properties:Option[Mapping.Properties] = None) : AggregateMapping = {
        AggregateMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier.parse(context.evaluate(input)),
            dimensions.map(context.evaluate),
            ListMap(aggregations.toSeq.map { case(k,v) => k -> context.evaluate(v) }:_*),
            context.evaluate(filter),
            if (partitions.isEmpty) 0 else context.evaluate(partitions).toInt
        )
    }
}
