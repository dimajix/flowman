/*
 * Copyright 2019 Kaya Kupferschmidt
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


class SelectorMetricBundle(override val name:String, override val labels:Map[String,String], registry:MetricSystem, selector:Selector, relabel:Map[String,String] => Map[String,String] = identity) extends MetricBundle {
    /**
      * Returns all metrics in this bundle. This operation may be expensive, since the set of metrics may be
      * dynamic and change over time. Therefore you should not cache the result of this method since it may be
      * invalid over time
      * @return
      */
    override def metrics : Seq[Metric] = registry.findMetric(selector)
        .map(relabelMetric)

    /**
      * Resets and/or removes all metrics in this bundle.
      */
    override def reset() : Unit = {
        registry.findBundle(selector).foreach(_.reset())
        registry.findMetric(selector).foreach(_.reset())
    }

    private def relabelMetric(metric:Metric) = metric match {
        case gauge:GaugeMetric => new FixedGaugeMetric(name, relabel(gauge.labels), gauge.value)
        case _ => throw new IllegalArgumentException(s"Metric of type ${metric.getClass} not supported")
    }

    //private def relabel(labels:Map[String,String]) : Map[String,String] = {
    //    metricLabels.map { case(k,v) => (k, context.evaluate(v, labels)) }
    //}
}
