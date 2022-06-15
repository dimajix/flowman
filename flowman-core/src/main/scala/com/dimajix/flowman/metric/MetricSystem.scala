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

import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.slf4j.LoggerFactory

import com.dimajix.common.IdentityHashSet
import com.dimajix.common.SynchronizedSet
import com.dimajix.flowman.execution.Status


trait MetricCatalog {
    /**
     * returns all metrics
     * @return
     */
    def metrics : Seq[Metric]

    /**
     * Returns all currently registered MetricBundles
     * @return
     */
    def bundles : Seq[MetricBundle]

    /**
     * Finds all metrics with the given attributes. All returned metrics will contain the specified
     * key-value pairs as labels, although the metrics may contain additional labels
     * @param selector
     * @return
     */
    def findMetric(selector:Selector) : Seq[Metric]

    /**
     * Finds all metric bundles with the given attributes. All returned bundles will contain the specified
     * key-value pairs as labels, although the metric bundles may contain additional labels
     * @param selector
     * @return
     */
    def findBundle(selector:Selector) : Seq[MetricBundle]
}


class MetricSystem extends MetricCatalog {
    private val logger = LoggerFactory.getLogger(getClass)
    private val metricBundles : SynchronizedSet[MetricBundle] = SynchronizedSet(IdentityHashSet())
    private val metricBoards : SynchronizedSet[MetricBoard] = SynchronizedSet(IdentityHashSet())
    private val metricSinks : SynchronizedSet[MetricSink] = SynchronizedSet(IdentityHashSet())

    /**
     * Registers an individual metric. It will be wrapped into a bundle.
     * @param metric
     */
    def addMetric(metric:Metric) : Unit = {
        metricBundles.add(SingletonMetricBundle(metric))
    }

    /**
      * Registers a new MetricBundle
      * @param bundle
      */
    def addBundle(bundle:MetricBundle) : Unit = {
        metricBundles.add(bundle)
    }

    /**
      * Removes a bundle again from the registry
      * @param bundle
      */
    def removeBundle(bundle:MetricBundle) : Unit = {
        metricBundles.remove(bundle)
    }

    def getOrCreateBundle[T <: MetricBundle](name:String, labels:Map[String,String], creator: => T) : T = {
        metricBundles.find(bundle => name == bundle.name && bundle.labels == labels)
            .map(_.asInstanceOf[T])
            .getOrElse{
                val bundle = creator
                if (name != bundle.name || labels != bundle.labels)
                    throw new IllegalArgumentException("Newly created bundle needs to match query")
                addBundle(bundle)
                bundle
            }
    }

    /**
      * Returns all currently registered MetricBundles
      * @return
      */
    override def bundles : Seq[MetricBundle] = metricBundles.toSeq

    /**
     * returns all metrics
     * @return
     */
    override def metrics : Seq[Metric] = metricBundles.toSeq.flatMap(_.metrics)

    /**
      * Adds a new MetricBoard to the system. The board is also added to all previously registered sinks.
      * @param board
      */
    def addBoard(board:MetricBoard) : Unit = {
        if (metricBoards.add(board)) {
            metricSinks.foreach(_.addBoard(board, this))
        }
    }

    /**
      * Adds a new MetricBoard to the system. The board is also removed to all previously registered sinks.
      * @param board
      */
    def removeBoard(board:MetricBoard) : Unit = {
        if (metricBoards.remove(board)) {
            metricSinks.foreach(_.removeBoard(board))
        }
    }

    /**
      * Commits a previously registered MetricBoard in all registered metric sinks
      * @param board
      */
    def commitBoard(board:MetricBoard, status:Status) : Unit = {
        if (!metricBoards.contains(board))
            throw new IllegalArgumentException("MetricBoard not registered")

        metricSinks.foreach { sink =>
            try {
                sink.commit(board, status)
            }
            catch {
                case NonFatal(ex) => logger.warn(s"Error while committing metrics to sink: ${ex.getMessage}")
            }
        }
    }

    /**
      * Adds a MetricSink to the registry. This will only add the sink, which then will be called on each
      * commitMetrics invocation. This will also add all metric boards to the sink
      * @param sink
      */
    def addSink(sink:MetricSink) : Unit = {
        if (metricSinks.add(sink)) {
            metricBoards.foreach(b => sink.addBoard(b, this))
        }
    }

    /**
      * Removes a MetricSink again from the registry. This will also remove all metric boards from the sink
      * @param sink
      */
    def removeSink(sink:MetricSink) : Unit = {
        if (metricSinks.remove(sink)) {
            metricBoards.foreach(b => sink.removeBoard(b))
        }
    }

    /**
      * Returns a list of all registered metric sinks
      * @return
      */
    def sinks : Seq[MetricSink] = metricSinks.toSeq

    /**
      * Resets all metric bundles in this registry
      */
    def resetMetrics() : Unit = {
        metricBundles.foreach(_.reset())
    }

    /**
      * Finds all metrics with the given attributes. All returned metrics will contain the specified
      * key-value pairs as labels, although the metrics may contain additional labels
      * @param selector
      * @return
      */
    override def findMetric(selector:Selector) : Seq[Metric] = {
        require(selector != null)

        // Matches bundle labels to query. Only existing labels need to match
        def matchBundle(bundle:MetricBundle) : Boolean = {
            val labels = bundle.labels
            selector.name.forall(_.unapplySeq(bundle.name).nonEmpty) &&
                labels.keySet.intersect(selector.labels.keySet).forall(key => selector.labels(key).unapplySeq(labels(key)).nonEmpty)
        }
        // Matches metric labels to query. All labels need to match
        def matchMetric(metric:Metric, query:Map[String,Regex]) : Boolean = {
            val labels = metric.labels
            query.forall(kv => labels.get(kv._1).exists(v => kv._2.unapplySeq(v).nonEmpty))
        }
        // Query a bundle and return all matching metrics within that bundle
        def queryBundle(bundle:MetricBundle) : Seq[Metric] = {
            val prunedQuery = selector.labels.keySet.diff(bundle.labels.keySet)
                .map(key => key -> selector.labels(key))
                .toMap

            bundle.metrics.filter(m => matchMetric(m, prunedQuery))
        }

        metricBundles.toSeq
            .filter(b => matchBundle(b))
            .flatMap(queryBundle)
    }

    /**
      * Finds all metric bundles with the given attributes. All returned bundles will contain the specified
      * key-value pairs as labels, although the metric bundles may contain additional labels
      * @param selector
      * @return
      */
    override def findBundle(selector:Selector) : Seq[MetricBundle] = {
        require(selector != null)

        def matchBundle(bundle:MetricBundle) : Boolean = {
            val labels = bundle.labels
            selector.name.forall(_.unapplySeq(bundle.name).nonEmpty) &&
                selector.labels.forall(kv => labels.get(kv._1).exists(v => kv._2.unapplySeq(v).nonEmpty))
        }

        metricBundles.toSeq
            .filter(b => matchBundle(b))
    }
}
