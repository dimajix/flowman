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

package com.dimajix.flowman.spec.metric

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.metric.FixedGaugeMetric
import com.dimajix.flowman.metric.GaugeMetric
import com.dimajix.flowman.metric.Metric
import com.dimajix.flowman.metric.MetricRegistry


case class MetricFamily(
    context: Context,
    name:String,
    selector:Map[String,String],
    labels:Map[String,String]
) {
    def metrics(registry:MetricRegistry) : Seq[Metric] = {
        registry.findMetric(selector)
            .map(relabelMetric)
    }

    private def relabelMetric(metric:Metric) : Metric = {
        metric match {
            case gauge:GaugeMetric => new FixedGaugeMetric(gauge.value, relabel(gauge.labels))
            case _ => throw new IllegalArgumentException(s"Metric of type ${metric.getClass} not supported")
        }
    }

    private def relabel(labels:Map[String,String]) : Map[String,String] = {
        this.labels.mapValues(v => context.evaluate(v, labels))
    }
}



class MetricFamilySpec {
    @JsonProperty(value = "name", required = true) private[spec] var _name:String = _
    @JsonProperty(value = "selector", required = true) private[spec] var _selector:Map[String,String] = Map()
    @JsonProperty(value = "labels", required = true) private[spec] var _labels:Map[String,String] = Map()

}
