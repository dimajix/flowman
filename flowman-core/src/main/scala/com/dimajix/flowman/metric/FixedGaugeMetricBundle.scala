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

import scala.collection.mutable


class FixedGaugeMetricBundle(bundleLabels:Map[String,String], metricKey: String) extends MetricBundle {
    private val gauges = mutable.Map[String, FixedGaugeMetric]()

    /**
      * Returns all labels associated with this metrics. A label is an arbitrary key-value pair used to
      * distinguish between different metrics. They are also used in the MetricRegistry to find specific metrics
      * and metric bundles
      * @return
      */
    override def labels: Map[String, String] = bundleLabels

    /**
      * Returns all metrics in this bundle. This operation may be expensive, since the set of metrics may be
      * dynamic and change over time
      * @return
      */
    override def metrics: Seq[Metric] = {
        gauges.values.toSeq
    }

    /**
      * Resets and/or removes all metrics in this bundle.
      */
    override def reset(): Unit = {
        gauges.clear()
    }

    def update(name:String, value:Double) = {
        gauges.update(name, new FixedGaugeMetric(value, bundleLabels.updated(metricKey, name)))
    }
}
