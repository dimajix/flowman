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

import scala.collection.mutable


final case class FixedGaugeMetricBundle(override val name:String, override val labels:Map[String,String], metricKey: String) extends MetricBundle {
    private val gauges = mutable.Map[String, FixedGaugeMetric]()

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

    /**
      * Updates an individual value using the specified name
      * @param name
      * @param value
      */
    def update(name:String, value:Double) : Unit = {
        gauges.update(name, new FixedGaugeMetric(this.name, labels.updated(metricKey, name), value))
    }
}
