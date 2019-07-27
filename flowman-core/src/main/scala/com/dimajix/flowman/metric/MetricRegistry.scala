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

import scala.collection.mutable


class MetricRegistry {
    private val bundles : mutable.Set[MetricBundle] = mutable.Set()

    /**
      * Registers a new MetricBundle
      * @param bundle
      */
    def register(bundle:MetricBundle) : Unit = {
        // Ensure that a single bundle is not added multiple times.
        if (bundles.contains(bundle))
            throw new IllegalArgumentException("MetricBundle already registered")
        bundles += bundle
    }

    /**
      * Resets all metric bundles in this registry
      */
    def reset() : Unit = {
        bundles.foreach(_.reset())
    }

    /**
      * Finds all metrics with the given attributes. All returned metrics will contain the specified
      * key-value pairs as labels, although the metrics may contain additional labels
      * @param query
      * @return
      */
    def findMetric(query:Map[String,String]) : Seq[Metric] = {
        // Matches bundle labels to query. Only existing labels need to match
        def matchBundle(bundle:MetricBundle, query:Map[String,String]) : Boolean = {
            val labels = bundle.labels
            labels.keySet.intersect(query.keySet)
                .forall(key => query(key) == labels(key))
        }
        // Matches metric labels to query. All labels need to match
        def matchMetric(metric:Metric, query:Map[String,String]) : Boolean = {
            val labels = metric.labels
            query.forall(kv => labels.get(kv._1).contains(kv._2))
        }
        // Query a bundle and return all matching metrics within that bundle
        def queryBundle(bundle:MetricBundle, query:Map[String,String]) : Seq[Metric] = {
            val prunedQuery = query.keySet.diff(bundle.labels.keySet)
                .map(key => key -> query(key))
                .toMap

            bundle.metrics.filter(m => matchMetric(m, prunedQuery))
        }

        bundles.toSeq
            .filter(b => matchBundle(b, query))
            .flatMap(queryBundle(_, query))
    }

    /**
      * Finds all metric bundles with the given attributes. All returned bundles will contain the specified
      * key-value pairs as labels, although the metric bundles may contain additional labels
      * @param query
      * @return
      */
    def findBundle(query:Map[String,String]) : Seq[MetricBundle] = {
        def matchBundle(bundle:MetricBundle, query:Map[String,String]) : Boolean = {
            val labels = bundle.labels
            query.forall(kv => labels.get(kv._1).contains(kv._2))
        }

        bundles.toSeq
            .filter(b => matchBundle(b, query))
    }

    /**
      * returns all metrics
      * @return
      */
    def metrics : Seq[Metric] = {
        bundles.toSeq.flatMap(_.metrics)
    }
}
