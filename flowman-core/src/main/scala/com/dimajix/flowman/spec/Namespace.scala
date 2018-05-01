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

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.runner.Runner
import com.dimajix.flowman.storage.Store
import com.dimajix.flowman.util.splitSettings


object Namespace {
    class Reader {
        private val logger = LoggerFactory.getLogger(classOf[Project])

        def file(file: File): Namespace = {
            logger.info(s"Reading namespace file ${file.toString}")
            ObjectMapper.read[Namespace](file)
        }
        def file(filename:String) : Namespace = {
            file(new File(filename))
        }
        def string(text:String) : Namespace = {
            ObjectMapper.parse[Namespace](text)
        }
        def default() : Namespace = {
            Option(System.getenv("FLOWMAN_CONF_DIR"))
                    .filter(_.nonEmpty)
                    .map(confDir => new File(confDir, "flowman-core.yml"))
                    .filter(_.isFile)
                    .map(file => ObjectMapper.read[Namespace](file))
                    .orNull
        }
    }

    def read = new Reader
}


class Namespace {
    @JsonProperty(value="store") private var _store: Store = _
    @JsonProperty(value="name") private var _name: String = "default"
    @JsonProperty(value="environment") private var _environment: Seq[String] = Seq()
    @JsonProperty(value="config") private var _config: Seq[(String,String)] = Seq()
    @JsonProperty(value="profiles") private var _profiles: Map[String,Profile] = Map()
    @JsonProperty(value="runner") private var _runner : Runner = _

    def name : String = _name

    def config : Seq[(String,String)] = _config
    def environment : Seq[(String,String)] = splitSettings(_environment)

    def profiles : Map[String,Profile] = _profiles
    def projects : Seq[String] = ???
    def runner : Runner = _runner
}
