/*
 * Copyright 2020-2021 Kaya Kupferschmidt
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

import scala.collection.JavaConverters._


final case class MetricWrapper(metric:Metric) {
    def getName() : String = metric.name
    def getLabels() : java.util.Map[String,String] = metric.labels.asJava
    def getValue() : Any = metric match {
        case m:GaugeMetric => m.value
        case _ => null
    }
}
