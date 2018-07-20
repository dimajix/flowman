/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier


class AggregateMapping extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[AggregateMapping])

    @JsonProperty(value = "input", required = true) private[spec] var _input:String = _
    @JsonProperty(value = "dimensions", required = true) private[spec] var _dimensions:Array[String] = _
    @JsonProperty(value = "aggregations", required = true) private[spec] var _aggregations:Map[String,String] = _
    @JsonProperty(value = "partitions", required = false) private[spec] var _partitions:String = _

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def dimensions(implicit context: Context) : Seq[String] = _dimensions.map(context.evaluate)
    def aggregations(implicit context: Context) : Map[String,String] = _aggregations.mapValues(context.evaluate)
    def partitions(implicit context: Context) : Int = if (_partitions == null || _partitions.isEmpty) 0 else context.evaluate(_partitions).toInt

    /**
      * Creates an instance of the aggregated table.
      *
      * @param executor
      * @param tables
      * @return
      */
    override def execute(executor:Executor, tables:Map[MappingIdentifier,DataFrame]): DataFrame = {
        implicit val context = executor.context
        val input = this.input
        val dimensions = this.dimensions
        logger.info(s"Aggregating mapping '$input' on dimensions ${dimensions.mkString(",")}")

        val df = tables(input)
        val dims = dimensions.map(col)
        val aggs = aggregations.map(kv => expr(kv._2).as(kv._1))
        val parts = partitions
        if (parts > 0)
            df.repartition(parts, dims:_*).groupBy(dims:_*).agg(aggs.head, aggs.tail.toSeq:_*)
        else
            df.groupBy(dims:_*).agg(aggs.head, aggs.tail.toSeq:_*)
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context:Context) : Array[MappingIdentifier] = {
        Array(input)
    }
}
