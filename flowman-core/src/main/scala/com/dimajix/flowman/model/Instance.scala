/*
 * Copyright (C) 2018 The Flowman Authors
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


object Prototype {
    implicit def of[T <: Instance](instance:T) : Prototype[T] = new Prototype[T] {
        override def instantiate(context: Context, properties:Option[T#PropertiesType] = None): T = instance
    }
    implicit def of[T <: Instance](fn:Context => T) : Prototype[T] = new Prototype[T] {
        override def instantiate(context: Context, properties:Option[T#PropertiesType] = None): T = fn(context)
    }
}
trait Prototype[T <: Instance] {
    def instantiate(context: Context, properties:Option[T#PropertiesType] = None) : T
}


trait Instance {
    // Don't create a generic, this would overcomplicate using the Prototype trait
    type PropertiesType <: Properties[PropertiesType]

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
    def category : Category

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
    def metadata : Metadata
}


abstract class AbstractInstance extends Instance {
    protected def instanceProperties : PropertiesType

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

    /**
     * Returns the full set of meta data of this resource
     * @return
     */
    override def metadata : Metadata = instanceProperties.metadata
}
