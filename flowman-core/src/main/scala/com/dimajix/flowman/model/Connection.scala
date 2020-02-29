/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.model

import com.dimajix.flowman.execution.Context


object Connection {
    object Properties {
        def apply(context: Context, name:String = "") : Properties = {
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                "",
                Map()
            )
        }
    }
    final case class Properties(
        context: Context,
        namespace: Option[Namespace],
        project: Option[Project],
        name: String,
        kind: String,
        labels: Map[String, String]
    ) extends Instance.Properties
}

/**
 * Base class to be used for all Connection instances
 */
trait Connection extends Instance {
    /**
     * Returns the category of this resource
     *
     * @return
     */
    final override def category: String = "connection"

    /**
     * Returns an identifier for this connection
     *
     * @return
     */
    def identifier: ConnectionIdentifier
}


abstract class BaseConnection extends AbstractInstance with Connection {
    protected override def instanceProperties : Connection.Properties

    /**
     * Returns an identifier for this connection
     * @return
     */
    def identifier : ConnectionIdentifier = ConnectionIdentifier(name, project.map(_.name))
}
