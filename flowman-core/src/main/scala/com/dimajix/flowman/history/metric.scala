/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman.history

import java.time.Clock
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.metric.GaugeMetric
import com.dimajix.flowman.metric.Metric


object Measurement {
    def ofMetrics(metrics: Seq[Metric]): Seq[Measurement] = {
        val now = Clock.systemDefaultZone().instant().atZone(ZoneId.systemDefault()).truncatedTo(ChronoUnit.MILLIS)
        metrics.flatMap {
            case gauge: GaugeMetric => Some(Measurement(gauge.name, "", now, gauge.labels, gauge.value))
            case _ => None
        }
    }
}

final case class Measurement(
    name: String,
    jobId: String,
    ts: ZonedDateTime,
    labels: Map[String, String],
    value: Double
)


final case class MetricSeries(
    metric: String,
    namespace: String,
    project: String,
    job: String,
    phase: Phase,
    labels: Map[String, String],
    measurements: Seq[Measurement]
)
