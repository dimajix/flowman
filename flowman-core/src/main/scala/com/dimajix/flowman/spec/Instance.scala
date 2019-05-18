/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec

import com.dimajix.flowman.execution.Context


object Instance {
    trait Properties {
        val context: Context
        val name: String
        val kind: String
        val labels: Map[String, String]
    }
}

abstract class Instance {
    /**
      * Returns the name of the resource
      * @return
      */
    def name : String

    /**
      * Returns the kind of the resource
      * @return
      */
    def kind : String

    /**
      * Returns the category of the resource
      * @return
      */
    def category : String

    /**
      * Returns the map of generic meta data labels
      * @return
      */
    def labels : Map[String,String]

    /**
      * Returns the Namespace this instance belongs to
      * @return
      */
    def namespace : Namespace = context.namespace

    /**
      * Returns the Project this instance belongs to
      */
    def project : Project = context.project

    /**
      * Returns the context that was used to create this instance
      */
    def context : Context

    /**
      * Returns the full set of meta data of this resource
      * @return
      */
    def metadata : Metadata = {
        Metadata(
            Option(context.namespace).map(_.name),
            Option(context.project).map(_.name),
            name,
            Option(context.project).map(_.version),
            category,
            kind,
            labels
        )
    }
}


abstract class AbstractInstance extends Instance {
    protected def instanceProperties : Instance.Properties

    /**
      * Returns the name of the resource
      * @return
      */
    override def name : String = instanceProperties.name

    /**
      * Returns the kind of the resource
      * @return
      */
    override def kind : String = instanceProperties.kind

    /**
      * Returns the map of generic meta data labels
      * @return
      */
    override def labels : Map[String,String] = instanceProperties.labels

    /**
      * Returns the context that was used to create this instance
      */
    override def context : Context = instanceProperties.context
}
