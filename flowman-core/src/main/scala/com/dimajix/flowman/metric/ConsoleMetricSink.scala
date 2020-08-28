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


class ConsoleMetricSink extends AbstractMetricSink {
    override def commit(board:MetricBoard): Unit = {
        implicit val catalog = this.catalog(board)
        board.metrics.foreach{ metric =>
            val name = metric.name
            val labels = metric.labels.map(kv => kv._1 + "=" + kv._2)
            metric match {
                case gauge: GaugeMetric => println(s"MetricSelection($name) GaugeMetric(${labels.mkString(",")})=${gauge.value}")
                case _: Metric => println(s"MetricSelection($name) Metric(${labels.mkString})=???")
            }
        }
    }
}
