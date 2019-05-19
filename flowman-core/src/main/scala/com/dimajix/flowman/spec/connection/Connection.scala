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

package com.dimajix.flowman.spec.connection

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.AbstractInstance
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.Instance
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spi.TypeRegistry


object Connection {
    object Properties {
        def apply(context:Context=null, name:String="", kind:String="") : Properties = {
            Properties(
                context,
                if (context != null) context.namespace else null,
                if (context != null) context.project else null,
                name,
                kind,
                Map()
            )
        }
    }
    case class Properties(
        context:Context,
        namespace:Namespace,
        project:Project,
        name:String,
        kind:String,
        labels:Map[String,String]
     ) extends Instance.Properties
}

/**
  * Base class to be used for all Connection instances
  */
abstract class Connection extends AbstractInstance {
    protected override def instanceProperties : Connection.Properties

    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: String = "connection"

    /**
      * Returns an identifier for this connection
      * @return
      */
    def identifier : ConnectionIdentifier
}



object ConnectionSpec extends TypeRegistry[ConnectionSpec] {
    type NameResolver = NamedSpec.NameResolver[Connection, ConnectionSpec]
}

/**
  * The ConnectionSpec class contains the raw specification values, which may require interpolation.
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", defaultImpl = classOf[JdbcConnectionSpec], visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "jdbc", value = classOf[JdbcConnectionSpec]),
    new JsonSubTypes.Type(name = "ssh", value = classOf[SshConnectionSpec]),
    new JsonSubTypes.Type(name = "sftp", value = classOf[SshConnectionSpec])
))
abstract class ConnectionSpec extends NamedSpec[Connection] {
    /**
      * Creates an instance of this specification and performs the interpolation of all variables
      * @param context
      * @return
      */
    override def instantiate(context:Context) : Connection

    /**
      * Returns a set of common properties
      * @param context
      * @return
      */
    override protected def instanceProperties(context:Context) : Connection.Properties = {
        require(context != null)
        Connection.Properties(
            context,
            context.namespace,
            context.project,
            name,
            kind,
            labels.mapValues(context.evaluate)
        )
    }
}
