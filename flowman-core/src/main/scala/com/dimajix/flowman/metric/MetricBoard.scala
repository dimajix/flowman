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

/**
 * A MetricBoard is a collection of multiple MetricBundles to be published together to one or multiple MetricSinks
 * @param labels
 * @param selections
 */
final case class MetricBoard(
    labels:Map[String,String],
    selections:Seq[MetricSelection]
) {
    /**
     * Resets all Metrics and MetricBundles matching the selections of the board
     * @param catalog
     */
    def reset(catalog:MetricCatalog) : Unit = {
        metrics(catalog).foreach(_.reset())
        bundles(catalog).foreach(_.reset())
    }

    /**
     * Returns all Metrics matching the selections of the board
     * @param catalog
     */
    def metrics(implicit catalog:MetricCatalog) : Seq[Metric] = selections.flatMap(_.metrics)

    /**
     * Returns all MetricBundles matching the selections of the board
     * @param catalog
     */
    def bundles(implicit catalog:MetricCatalog) : Seq[MetricBundle] = selections.flatMap(_.bundles)
}


/**
 * A MetricSelection represents a possibly dynamic set of Metrics to be published inside a MetricBoard
 */
final case class MetricSelection(name:String, selector:Selector, relabel:Map[String,String] => Map[String,String] = identity) {
    /**
     * Returns all metrics identified by this selection. This operation may be expensive, since the set of metrics may be
     * dynamic and change over time
     * @return
     */
    def metrics(implicit catalog:MetricCatalog) : Seq[Metric] = catalog.findMetric(selector)
        .map(relabelMetric)

    /**
     * Returns all bundles identified by this selection. This operation may be expensive, since the set of metrics may be
     * dynamic and change over time
     * @return
     */
    def bundles(implicit catalog:MetricCatalog) : Seq[MetricBundle] = catalog.findBundle(selector)

    private def relabelMetric(metric:Metric) = metric match {
        case gauge:GaugeMetric => new FixedGaugeMetric(name, relabel(gauge.labels), gauge.value)
        case _ => throw new IllegalArgumentException(s"Metric of type ${metric.getClass} not supported")
    }
}


final case class Selector(
    name:Option[String] = None,
    labels:Map[String,String] = Map()
) {
    require(name != null)
    require(labels != null)
}
