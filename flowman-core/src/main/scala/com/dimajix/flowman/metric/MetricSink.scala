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


abstract class MetricSink {
    /**
      * Adds a MetricBoard to be published by this sink. Publishing could happen on a periodic base in the background
      * or via an explicit call to commit
      * @param board
      * @param catalog
      */
    def addBoard(board:MetricBoard, catalog:MetricCatalog) : Unit

    /**
      * Removes a metric board  from the sink again
      * @param board
      */
    def removeBoard(board:MetricBoard) : Unit

    /**
      * Returns all metric boards currently registered to this sink
      * @return
      */
    def boards : Seq[MetricBoard]

    /**
      * Returns all metrics of all bundles currently registered to this sink
      * @return
      */
    def metrics : Seq[Metric]

    /**
      * Commits all metrics of a previously registered board. This may be required for some sink for example the
      * PrometheusSink which uses the Prometheus push gateway.
      */
    def commit(board:MetricBoard) : Unit
}
