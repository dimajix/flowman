/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.documentation

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.graph.Graph
import com.dimajix.flowman.model
import com.dimajix.flowman.model.AbstractInstance
import com.dimajix.flowman.model.Instance
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project


object Collector {
    final case class Properties(
        kind:String
    ) extends model.Properties[Properties] {
        override val context : Context = null
        override val namespace : Option[Namespace] = None
        override val project : Option[Project] = None
        override val name : String = ""
        override val metadata : Metadata = Metadata(name="", category=model.Category.DOCUMENTATION_COLLECTOR.lower, kind=kind)

        override def withName(name: String): Properties = ???
    }
}


trait Collector extends Instance {
    override type PropertiesType = Collector.Properties

    /**
     * Returns the category of the resource
     *
     * @return
     */
    override def category: model.Category = model.Category.DOCUMENTATION_COLLECTOR

    def collect(execution: Execution, graph:Graph, documentation:ProjectDoc) : ProjectDoc
}


abstract class AbstractCollector extends AbstractInstance with Collector {
    override protected def instanceProperties: Collector.Properties = Collector.Properties(kind)
}
