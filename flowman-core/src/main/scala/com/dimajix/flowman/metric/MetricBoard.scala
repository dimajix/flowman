/*
 * Copyright (C) 2019 The Flowman Authors
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

import scala.util.matching.Regex

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model
import com.dimajix.flowman.model.AbstractInstance
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project


object MetricBoard {
    final case class Properties(
        context:Context
    ) extends model.Properties[Properties] {
        override val namespace : Option[Namespace] = context.namespace
        override val project : Option[Project] = context.project
        override val name : String = ""
        override val kind : String = "metric_board"
        override val metadata : Metadata = Metadata(name="", category=Category.METRIC_BOARD.lower, kind=kind)

        override def withName(name: String): Properties = ???
    }
}

/**
 * A MetricBoard is a collection of multiple MetricBundles to be published together to one or multiple MetricSinks
 *
 * @param labels
 * @param selections
 */
final case class MetricBoard(
    instanceProperties:MetricBoard.Properties,
    labels:Map[String,String],
    selections:Seq[MetricSelection]
) extends AbstractInstance {
    override type PropertiesType = MetricBoard.Properties

    /**
     * Returns the category of the resource
     *
     * @return
     */
    override def category: Category = Category.METRIC_BOARD

    /**
     * Resets all Metrics and MetricBundles matching the selections of the board
     *
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
                // Remove "project" from gauge.labels
                case gauge:GaugeMetric => FixedGaugeMetric(sel.name.getOrElse(gauge.name), env.evaluate(labels ++ sel.labels, gauge.labels - "project" + ("status" -> status)), gauge.value)
                case _ => throw new IllegalArgumentException(s"Metric of type ${metric.getClass} not supported")
            }

            sel.metrics(catalog).map(relabel)
        }
    }
}


/**
 * A MetricSelection represents a possibly dynamic set of Metrics to be published inside a MetricBoard
 */
final case class MetricSelection(name:Option[String] = None, selector:Selector, labels:Map[String,String] = Map()) {
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

object Selector {
    def apply(labels:Map[String,String]) : Selector = {
        new Selector(None, labels.map { case(k,v) => k -> v.r } )
    }
    def apply(name:String) : Selector = {
        new Selector(Some(name.r), Map.empty )
    }
    def apply(name:String, labels:Map[String,String]) : Selector = {
        new Selector(Some(name.r), labels.map { case(k,v) => k -> v.r } )
    }
}
final case class Selector(
    name:Option[Regex] = None,
    labels:Map[String,Regex] = Map()
)
