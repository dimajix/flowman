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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.util.StdConverter

import com.dimajix.flowman.spec.connection.Connection
import com.dimajix.flowman.spec.connection.ConnectionSpec


object Profile {
    class NameResolver extends StdConverter[Map[String,Profile],Map[String,Profile]] {
        override def convert(value: Map[String,Profile]): Map[String,Profile] = {
            value.foreach(kv => kv._2._name = kv._1)
            value
        }
    }
}


class Profile {
    @JsonIgnore private var _name:String = ""
    @JsonProperty(value="enabled") private var _enabled : Boolean = true
    @JsonProperty(value="environment") private var _environment: Seq[String] = Seq()
    @JsonProperty(value="config") private var _config: Seq[String] = Seq()
    @JsonDeserialize(converter=classOf[ConnectionSpec.NameResolver])
    @JsonProperty(value="connections") private var _connections: Map[String,ConnectionSpec] = Map()

    /**
      * Returns the name of the profile
      * @return
      */
    def name : String = _name

    /**
      * Returns true if the profile is enabled
      *
      * @return
      */
    def enabled : Boolean = _enabled

    /**
      * Returns a map of all configured databases
      *
      * @return
      */
    def connections : Map[String,ConnectionSpec] = _connections

    /**
      * Returns all configuration variables as a key-value sequence
      *
      * @return
      */
    def config : Seq[(String,String)] = splitSettings(_config)

    /**
      * Returns the environment as a key-value-sequence
      *
      * @return
      */
    def environment : Seq[(String,String)] = splitSettings(_environment)
}
