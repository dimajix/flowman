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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.util.StdConverter

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.Resource
import com.dimajix.flowman.spi.TypeRegistry


object Connection extends TypeRegistry[Connection] {
    class NameResolver extends StdConverter[Map[String, Connection], Map[String, Connection]] {
        override def convert(value: Map[String, Connection]): Map[String, Connection] = {
            value.foreach(kv => kv._2._name = kv._1)
            value
        }
    }
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", defaultImpl = classOf[JdbcConnection], visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "jdbc", value = classOf[JdbcConnection]),
    new JsonSubTypes.Type(name = "ssh", value = classOf[SshConnection]),
    new JsonSubTypes.Type(name = "sftp", value = classOf[SshConnection])
))
abstract class Connection extends Resource {
    @JsonIgnore private var _name:String = ""

    @JsonProperty(value="kind", required = true) private var _kind: String = _
    @JsonProperty(value="labels", required=false) private var _labels:Map[String,String] = Map()

    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: String = "connection"

    /**
      * Returns the specific kind of this resource
      * @return
      */
    final override def kind: String = _kind

    /**
      * Returns the name of the connection
      * @return
      */
    final override def name : String = _name

    /**
      * Returns a map of user defined labels
      * @param context
      * @return
      */
    final override def labels(implicit context: Context) : Map[String,String] = _labels.mapValues(context.evaluate)
}
