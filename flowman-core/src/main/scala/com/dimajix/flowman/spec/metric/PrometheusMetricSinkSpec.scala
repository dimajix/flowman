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
import com.dimajix.flowman.metric.MetricSink
import com.dimajix.flowman.metric.PrometheusMetricSink


class PrometheusMetricSinkSpec extends MetricSinkSpec {
    @JsonProperty(value = "url", required = true) private var url:String = _
    @JsonProperty(value = "labels", required = false) var labels:Map[String,String] = Map()

    override def instantiate(context: Context): MetricSink = {
        new PrometheusMetricSink(
            context.evaluate(url),
            context.evaluate(labels)
        )
    }
}
