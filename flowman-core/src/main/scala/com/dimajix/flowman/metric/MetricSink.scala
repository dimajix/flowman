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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Instance
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project


object MetricSink {
    final case class Properties(
        kind:String
    ) extends model.Properties[Properties] {
        override val context : Context = null
        override val namespace : Option[Namespace] = None
        override val project : Option[Project] = None
        override val name : String = ""
        override val metadata : Metadata = Metadata(name="", category=model.Category.METRIC_SINK.lower, kind=kind)

        override def withName(name: String): Properties = ???
    }
}

trait MetricSink extends Instance {
    override type PropertiesType = MetricSink.Properties

    /**
     * Returns the category of the resource
     *
     * @return
     */
    final override def category: Category = Category.METRIC_SINK

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
      * Commits all metrics of a previously registered board. This may be required for some sink for example the
      * PrometheusSink which uses the Prometheus push gateway.
      */
    def commit(board:MetricBoard, status:Status) : Unit
}
