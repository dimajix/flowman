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

package com.dimajix.flowman.spec.mapping

import java.util.UUID

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.util.QueryExecutionListener

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.metric.Selector
import com.dimajix.flowman.metric.SettableGaugeMetric
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType


case class ObserveMapping(
    instanceProperties: Mapping.Properties,
    input: MappingOutputIdentifier,
    measures: Map[String,String]
) extends BaseMapping {
    /**
     * Returns the dependencies (i.e. names of tables in the Dataflow model)
     *
     * @return
     */
    override def inputs: Set[MappingOutputIdentifier] = Set(input)

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
        require(execution != null)
        require(input != null)

        val observationName = "flowman-" + UUID.randomUUID()

        val df = input(this.input)
        val exprs = measures.toSeq.map { case (name, e) => expr(e).as(name) }
        val result = SparkShim.observe(df, observationName, exprs.head, exprs.tail:_*)

        // Already add metrics in advance
        val metrics = execution.metricSystem
        val labels = this.metadata.asMap
        val metricMap = measures.keys.map { name =>
            name -> metrics.findMetric(Selector(name, labels))
                .headOption
                .collect { case x:SettableGaugeMetric => x }
                .getOrElse {
                    val metric = SettableGaugeMetric(name, labels)
                    metrics.addMetric(metric)
                    metric
                }
            }.toMap

        // Create listener for fetching the metrics
        val listener = new QueryExecutionListener {
            override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
                publishMetrics(qe, observationName, metricMap)
            }
            override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
                publishMetrics(qe, observationName, metricMap)
            }
        }
        result.sparkSession.listenerManager.register(listener)

        // Register listener for cleanup when the DataFrame is to be garbage collected
        execution.cleaner.registerQueryExecutionListener(result, listener)

        Map("main" -> result)
    }

    /**
     * Returns the schema as produced by this mapping, relative to the given input schema
     *
     * @param input
     * @return
     */
    override def describe(execution: Execution, input: Map[MappingOutputIdentifier, StructType]): Map[String, StructType] = {
        require(execution != null)
        require(input != null)

        val result = input(this.input)

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
    }

    private def publishMetrics(qe: QueryExecution, observationName: String, metricMap: Map[String, SettableGaugeMetric]): Unit = {
        def publishMetric(name: String, value: Double): Unit = {
            metricMap.get(name).foreach(_.value = value)
        }

        SparkShim.observedMetrics(qe).get(observationName)
            .foreach { row =>
                val values = row.getValuesMap[Any](row.schema.fieldNames)
                measures.keys.foreach { metricName =>
                    values.get(metricName).foreach {
                        case b: Byte => publishMetric(metricName, b)
                        case s: Short => publishMetric(metricName, s)
                        case i: Int => publishMetric(metricName, i)
                        case l: Long => publishMetric(metricName, l)
                        case f: Float => publishMetric(metricName, f)
                        case d: Double => publishMetric(metricName, d)
                        case d: java.math.BigDecimal => publishMetric(metricName, d.doubleValue())
                        case t: java.sql.Timestamp => publishMetric(metricName, t.getTime)
                        case _ =>
                    }
                }
            }
    }
}




class ObserveMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "measures", required = true) private var measures: Map[String,String] = Map.empty

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): ObserveMapping = {
        ObserveMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier(context.evaluate(input)),
            context.evaluate(measures)
        )
    }
}
