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

package com.dimajix.flowman.spec.metric

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.metric.MetricSink
import com.dimajix.flowman.spec.Spec
import com.dimajix.flowman.spec.annotation.MetricSinkType
import com.dimajix.flowman.spi.ClassAnnotationHandler


object MetricSinkSpec extends TypeRegistry[MetricSinkSpec] {
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "console", value = classOf[ConsoleMetricSinkSpec]),
    new JsonSubTypes.Type(name = "jdbc", value = classOf[JdbcMetricSinkSpec]),
    new JsonSubTypes.Type(name = "null", value = classOf[NullMetricSinkSpec]),
    new JsonSubTypes.Type(name = "prometheus", value = classOf[PrometheusMetricSinkSpec])
))
abstract class MetricSinkSpec extends Spec[MetricSink] {
    def instantiate(context:Context, properties:Option[MetricSink.Properties] = None) : MetricSink
}


class MetricSinkSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[MetricSinkType]

    override def register(clazz: Class[_]): Unit =
        MetricSinkSpec.register(clazz.getAnnotation(classOf[MetricSinkType]).kind(), clazz.asInstanceOf[Class[_ <: MetricSinkSpec]])
}
