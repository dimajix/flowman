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

import com.dimajix.common.IdentityHashSet
import com.dimajix.common.SynchronizedSet
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.model.Metadata


object MultiMetricBundle {
    def forMetadata(registry: MetricSystem, metricName: String, metadata : Metadata, phase:Phase) : MultiMetricBundle = {
        val bundleLabels =
            Map("phase" -> phase.upper) ++
                metadata.namespace.map("namespace" -> _).toMap ++
                metadata.project.map("project" -> _).toMap ++
                metadata.version.map("version" -> _).toMap
        registry.getOrCreateBundle(metricName, bundleLabels, MultiMetricBundle(metricName, bundleLabels))
    }
}

final case class MultiMetricBundle(override val name:String, override val labels:Map[String,String]) extends MetricBundle {
    private val bundleMetrics : SynchronizedSet[Metric] = SynchronizedSet(IdentityHashSet())

    def addMetric(metric:Metric) : Unit = {
        bundleMetrics.add(metric)
    }

    def removeMetric(metric:Metric) : Unit = {
        bundleMetrics.remove(metric)
    }

    def getOrCreateMetric[T <: Metric](name:String, labels:Map[String,String], creator: => T) : T = {
        bundleMetrics.find(metric => name == metric.name && metric.labels == labels)
            .map(_.asInstanceOf[T])
            .getOrElse{
                val metric = creator
                if (name != metric.name || labels != metric.labels)
                    throw new IllegalArgumentException("Newly created metric needs to match query")
                addMetric(metric)
                metric
            }
    }

    /**
      * Returns all metrics in this bundle. This operation may be expensive, since the set of metrics may be
      * dynamic and change over time
      *
      * @return
      */
    override def metrics: Seq[Metric] = bundleMetrics.toSeq

    /**
      * Resets and/or removes all metrics in this bundle.
      */
    override def reset(): Unit = {
        bundleMetrics.clear()
    }
}
