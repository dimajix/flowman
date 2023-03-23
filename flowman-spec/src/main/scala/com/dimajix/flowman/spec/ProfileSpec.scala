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

package com.dimajix.flowman.spec

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.util.StdConverter

import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.model.Profile
import com.dimajix.flowman.spec.connection.ConnectionSpec


object ProfileSpec {
    final class NameResolver extends StdConverter[Map[String,ProfileSpec],Map[String,ProfileSpec]] {
        override def convert(value: Map[String,ProfileSpec]): Map[String,ProfileSpec] = {
            value.foreach(kv => kv._2.name = kv._1)
            value
        }
    }
}


final class ProfileSpec {
    @JsonIgnore private var name:String = ""
    @JsonProperty(value="enabled") private var enabled : Boolean = false
    @JsonProperty(value="environment") private var environment: Seq[String] = Seq()
    @JsonProperty(value="config") private var config: Seq[String] = Seq()
    @JsonDeserialize(converter=classOf[ConnectionSpec.NameResolver])
    @JsonProperty(value="connections") private var connections: Map[String,ConnectionSpec] = Map()

    def instantiate() : Profile = {
        Profile(
            name,
            enabled,
            connections,
            splitSettings(config).toMap,
            splitSettings(environment).toMap
        )
    }
}
