/*
 * Copyright 2019-2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.metric

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Status


/**
 * A MetricBoard is a collection of multiple MetricBundles to be published together to one or multiple MetricSinks
 *
 * @param labels
 * @param selections
 */
final case class MetricBoard(
    context:Context,
    labels:Map[String,String],
    selections:Seq[MetricSelection]
) {
    /**
     * Resets all Metrics and MetricBundles matching the selections of the board
     * @param catalog
     */
    def reset(catalog:MetricCatalog) : Unit = {
        rawBundles(catalog).foreach(_.reset())
        rawMetrics(catalog).foreach(_.reset())
    }

    def rawMetrics(implicit catalog:MetricCatalog) : Seq[Metric] = selections.flatMap(_.metrics)
    def rawBundles(implicit catalog:MetricCatalog) : Seq[MetricBundle] = selections.flatMap(_.bundles)

    /**
     * Returns all Metrics matching the selections of the board. All labels will be evaluated. Note that the returned
     * metrics are not the original ones, but static copies with applied relabeling.
     * @param catalog
     */
    def metrics(catalog:MetricCatalog, status:Status) : Seq[Metric] = {
        val env = context.environment

        selections.flatMap { sel =>
            // Relabeling should happen has late as possible, since some values might be dynamic
            def relabel(metric:Metric) : Metric = metric match {
                case gauge:GaugeMetric => FixedGaugeMetric(sel.name, env.evaluate(labels ++ sel.labels, gauge.labels + ("status" -> status)), gauge.value)
                case _ => throw new IllegalArgumentException(s"Metric of type ${metric.getClass} not supported")
            }

            sel.metrics(catalog).map(relabel)
        }
    }
}


/**
 * A MetricSelection represents a possibly dynamic set of Metrics to be published inside a MetricBoard
 */
final case class MetricSelection(name:String, selector:Selector, labels:Map[String,String]) {
    /**
     * Returns all metrics identified by this selection. This operation may be expensive, since the set of metrics may be
     * dynamic and change over time
     * @return
     */
    def metrics(implicit catalog:MetricCatalog) : Seq[Metric] = catalog.findMetric(selector)

    /**
     * Returns all bundles identified by this selection. This operation may be expensive, since the set of metrics may be
     * dynamic and change over time
     * @return
     */
    def bundles(implicit catalog:MetricCatalog) : Seq[MetricBundle] = catalog.findBundle(selector)
}


final case class Selector(
    name:Option[String] = None,
    labels:Map[String,String] = Map()
)
