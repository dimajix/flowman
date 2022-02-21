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

package com.dimajix.flowman

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Result


package object metric {
    def withMetrics[T <: Result[T]](metricSystem: MetricSystem, metrics:Option[MetricBoard])(fn: => T) : T = {
        // Publish metrics
        metrics.foreach { metrics =>
            metrics.reset(metricSystem)
            metricSystem.addBoard(metrics)
        }

        // Run original function
        var status:Status = Status.UNKNOWN
        try {
            val result = fn
            status = result.status
            result
        }
        catch {
            case NonFatal(ex) =>
                status = Status.FAILED
                throw ex
        }
        finally {
            // Unpublish metrics
            metrics.foreach { metrics =>
                // Do not publish metrics for skipped jobs
                if (status != Status.SKIPPED) {
                    metricSystem.commitBoard(metrics, status)
                }
                metricSystem.removeBoard(metrics)
            }
        }
    }

    def withWallTime[T](registry: MetricSystem, metadata : Metadata, phase:Phase)(fn: => T) : T = {
        // Create and register bundle
        val metricName = metadata.category + "_runtime"
        val bundleLabels = metadata.asMap + ("phase" -> phase.toString)
        val bundle = registry.getOrCreateBundle(metricName, bundleLabels)(MultiMetricBundle(metricName, bundleLabels))

        // Create and register metric
        val metricLabels = bundleLabels ++ Map("name" -> metadata.name) ++ metadata.labels
        val metric = bundle.getOrCreateMetric(metricName, metricLabels)(WallTimeMetric(metricName, metricLabels))
        metric.reset()

        // Execute function itself, and catch any exception
        val result = Try {
            fn
        }

        metric.stop()

        // Rethrow original exception if some occurred
        result match {
            case Success(s) => s
            case Failure(ex) => throw ex
        }
    }
}
