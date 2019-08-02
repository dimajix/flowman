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

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.metric.MetricSink
import com.dimajix.flowman.spi.TypeRegistry


object MetricSinkSpec extends TypeRegistry[MetricSinkSpec] {
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "console", value = classOf[ConsoleMetricSinkSpec]),
    new JsonSubTypes.Type(name = "null", value = classOf[NullMetricSinkSpec]),
    new JsonSubTypes.Type(name = "prometheus", value = classOf[PrometheusMetricSinkSpec])
))
abstract class MetricSinkSpec {
    def instantiate(context:Context) : MetricSink
}
