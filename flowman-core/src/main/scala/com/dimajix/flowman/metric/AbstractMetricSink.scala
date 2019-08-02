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

import com.dimajix.common.IdentityHashSet


abstract class AbstractMetricSink extends MetricSink {
    private val metricBoards : mutable.Set[MetricBoard] = IdentityHashSet()

    /**
      * Adds a MetricBundle to be published by this sink. Publishing could happen on a periodic base in the background
      * or via an explicit call to commit
      * @param board
      */
    override def addBoard(board:MetricBoard) : Unit = {
        metricBoards.add(board)
    }

    /**
      * Removes a bundle  from the sink again
      * @param board
      */
    override def removeBoard(board:MetricBoard) : Unit = {
        metricBoards.remove(board)
    }

    /**
      * Returns all bundles currently registered to this sink
      * @return
      */
    override def boards : Seq[MetricBoard] = metricBoards.toSeq

    /**
      * Returns all bundles currently registered to this sink
      * @return
      */
    override def bundles : Seq[MetricBundle] = boards.flatMap(_.bundles)

    /**
      * Returns all metrics of all bundles currently registered to this sink
      * @return
      */
    override def metrics : Seq[Metric] = bundles.flatMap(_.metrics)

    /**
      * Commits all currently registered metrics. This may be required for some sink for example the PrometheusSink
      * which uses the Prometheus push gateway.
      */
    override def commit() : Unit = {
        metricBoards.foreach(board => commit(board))
    }
}
