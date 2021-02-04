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

package com.dimajix.flowman.model

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode

import com.dimajix.flowman.util.splitSettings


class Project {
    @JsonProperty(value="environment") private var _environment: Seq[String] = Seq()
    @JsonProperty(value="config") private var _config: Seq[String] = Seq()
    @JsonProperty(value="profiles") private var _profiles: Map[String,Profile] = Map()
    @JsonProperty(value="mappings") private var _mappings:Map[String,JsonNode] = Map()
    @JsonProperty(value="relations") private var _relations:Map[String,JsonNode] = Map()
    @JsonProperty(value="connections") private var _connections:Map[String,JsonNode] = Map()
    @JsonProperty(value="outputs") private var _outputs:Map[String,JsonNode] = Map()
    @JsonProperty(value="jobs") private var _jobs:Map[String,JsonNode] = Map()

    def profiles : Map[String,Profile] = _profiles
    def relations : Map[String,JsonNode] = _relations
    def connections : Map[String,JsonNode] = _connections
    def mappings : Map[String,JsonNode] = _mappings
    def outputs : Map[String,JsonNode] = _outputs
    def jobs : Map[String,JsonNode] = _jobs
    def config : Seq[(String,String)] = splitSettings(_config)
    def environment : Seq[(String,String)] = splitSettings(_environment)
}
