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

package com.dimajix.flowman.metric

import scala.collection.mutable

import com.dimajix.common.IdentityHashMap
import com.dimajix.flowman.model.AbstractInstance


abstract class AbstractMetricSink extends AbstractInstance with MetricSink {
    private val metricBoards : mutable.Map[MetricBoard,MetricCatalog] = IdentityHashMap()

    /**
      * Adds a MetricBundle to be published by this sink. Publishing could happen on a periodic base in the background
      * or via an explicit call to commit
      * @param board
      */
    override def addBoard(board:MetricBoard, catalog:MetricCatalog) : Unit = {
        if (metricBoards.contains(board))
            throw new IllegalArgumentException("MetricBoard already added to Sink")
        metricBoards.put(board, catalog)
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
    override def boards : Seq[MetricBoard] = metricBoards.keys.toSeq

    protected def catalog(board:MetricBoard) : MetricCatalog = {
        metricBoards.getOrElse(board, throw new IllegalArgumentException("Board not registered"))
    }
}
