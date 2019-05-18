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

package com.dimajix.flowman.spec.connection

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.AbstractInstance
import com.dimajix.flowman.spec.Instance
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spi.TypeRegistry


object Connection {
    case class Properties(
         context:Context,
         name:String="",
         kind:String="",
         labels:Map[String,String]=Map()
     ) extends Instance.Properties
}

abstract class Connection extends AbstractInstance {
    protected override def instanceProperties : Connection.Properties

    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: String = "connection"
}


object ConnectionSpec extends TypeRegistry[ConnectionSpec] {
    type NameResolver = NamedSpec.NameResolver[Connection, ConnectionSpec]
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", defaultImpl = classOf[JdbcConnectionSpec], visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "jdbc", value = classOf[JdbcConnectionSpec]),
    new JsonSubTypes.Type(name = "ssh", value = classOf[SshConnectionSpec]),
    new JsonSubTypes.Type(name = "sftp", value = classOf[SshConnectionSpec])
))
abstract class ConnectionSpec extends NamedSpec[Connection] {
    override def instantiate(context:Context) : Connection

    /**
      * Returns a set of common properties
      * @param context
      * @return
      */
    override protected def instanceProperties(context:Context) : Connection.Properties = {
        val name = this.name
        val kind = this.kind
        val labels = this.labels.mapValues(context.evaluate)
        Connection.Properties(context, name, kind, labels)
    }
}
