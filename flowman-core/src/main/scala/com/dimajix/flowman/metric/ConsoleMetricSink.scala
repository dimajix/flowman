/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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

import com.dimajix.flowman.execution.Status


class ConsoleMetricSink extends AbstractMetricSink {
    override def commit(board:MetricBoard, status:Status): Unit = {
        println("Collected metrics")
        board.metrics(catalog(board), status).sortBy(_.name).foreach{ metric =>
            val name = metric.name
            val labels = metric.labels.map(kv => kv._1 + "=" + kv._2)
            metric match {
                case gauge: GaugeMetric => println(s"  $name(${labels.mkString(",")}) = ${gauge.value}")
                case _: Metric => println(s"  $name(${labels.mkString}) = ???")
            }
        }
    }
}
