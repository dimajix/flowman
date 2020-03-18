/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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


object Template {
    implicit def of[T](instance:T) : Template[T] = new Template[T] {
        override def instantiate(context: Context): T = instance
    }
    implicit def of[T](fn:Context => T) : Template[T] = new Template[T] {
        override def instantiate(context: Context): T = fn(context)
    }
}
trait Template[T] {
    def instantiate(context: Context) : T
}


object Instance {
    trait Properties[T <: Properties[T]] {
        val context: Context
        val namespace: Option[Namespace]
        val project: Option[Project]
        val name: String
        val kind: String
        val labels: Map[String, String]

        def withName(name:String) : T
    }
}

trait Instance {
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
    def namespace : Option[Namespace]

    /**
      * Returns the Project this instance belongs to
      */
    def project : Option[Project]

    /**
      * Returns the Context that was used to create this instance and that may be used to perform
      * lookups of other resources
      */
    def context : Context

    /**
      * Returns the full set of meta data of this resource
      * @return
      */
    def metadata : Metadata = {
        Metadata(
            namespace.map(_.name),
            project.map(_.name),
            name,
            project.flatMap(_.version),
            category,
            kind,
            labels
        )
    }
}


abstract class AbstractInstance extends Instance {
    protected def instanceProperties : Instance.Properties[_]

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
      * Returns the Namespace this instance belongs to.
      * @return
      */
    override def namespace : Option[Namespace] = instanceProperties.namespace

    /**
      * Returns the Project this instance belongs to.
      */
    override def project : Option[Project] = instanceProperties.project

    /**
      * Returns the Context that was used to create this instance and that may be used to perform
      * lookups of other resources.
      */
    override def context : Context = instanceProperties.context
}
