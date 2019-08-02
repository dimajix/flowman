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

import com.dimajix.spark.accumulator.CounterAccumulator


class CounterAccumulatorMetricBundle(override val name:String, override val labels:Map[String,String], counters:CounterAccumulator, metricKey: String) extends MetricBundle {
    /**
      * Returns all metrics in this bundle. This operation may be expensive, since the set of metrics may be
      * dynamic and change over time
      * @return
      */
    override def metrics: Seq[Metric] = {
        counters.value.keys.toSeq
            .map(name => new Gauge(name))
    }

    override def reset() : Unit = {
        counters.reset
    }

    private class Gauge(label:String) extends GaugeMetric {
        private val metricLabels = CounterAccumulatorMetricBundle.this.labels.updated(metricKey, label)

        override def value: Double = counters.get(label).getOrElse(0l).toDouble

        override def name: String = CounterAccumulatorMetricBundle.this.name

        override def labels: Map[String, String] = metricLabels

        override def reset(): Unit = ???
    }
}
