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

package com.dimajix.flowman.spec

import java.io.File
import java.io.InputStream
import java.net.URL

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.catalog.CatalogSpec
import com.dimajix.flowman.spec.connection.ConnectionSpec
import com.dimajix.flowman.spec.history.HistorySpec
import com.dimajix.flowman.spec.history.NullHistorySpec
import com.dimajix.flowman.spec.metric.MetricSinkSpec
import com.dimajix.flowman.spec.metric.NullMetricSinkSpec
import com.dimajix.flowman.spec.storage.StorageSpec


object Namespace {
    class Builder {
        private val namespace = new Namespace

        def build() : Namespace = namespace

        def setName(name:String) : Builder = {
            namespace._name = name
            this
        }
        def setStateStore(monitor: HistorySpec) : Builder = {
            namespace._history = monitor
            this
        }
        def setCatalog(catalog: CatalogSpec) : Builder = {
            namespace._catalog = catalog
            this
        }
        def setEnvironment(env:Seq[(String,String)]) : Builder = {
            namespace._environment = env.map(kv => kv._1 + "=" + kv._2)
            this
        }
        def setConfig(conf:Seq[(String,String)]) : Builder = {
            namespace._config = conf.map(kv => kv._1 + "=" + kv._2)
            this
        }
        def setProfiles(profiles:Map[String,Profile]) : Builder = {
            namespace._profiles = profiles
            this
        }
        def addProfile(name:String, profile:Profile) : Builder = {
            namespace._profiles = namespace._profiles + (name -> profile)
            this
        }
        def setConnections(connections:Map[String,ConnectionSpec]) : Builder = {
            namespace._connections = connections
            this
        }
        def addConnection(name:String, connection:ConnectionSpec) : Builder = {
            namespace._connections = namespace._connections + (name -> connection)
            this
        }
    }

    class Reader {
        private val logger = LoggerFactory.getLogger(classOf[Namespace])

        def file(file: File): Namespace = {
            logger.info(s"Reading namespace file ${file.toString}")
            ObjectMapper.read[Namespace](file)
        }
        def file(filename:String) : Namespace = {
            file(new File(filename))
        }
        def stream(stream:InputStream) : Namespace = {
            ObjectMapper.read[Namespace](stream)
        }
        def url(url:URL) : Namespace = {
            logger.info(s"Reading namespace from url ${url.toString}")
            val con = url.openConnection()
            con.setUseCaches(false)
            stream(con.getInputStream)
        }
        def string(text:String) : Namespace = {
            ObjectMapper.parse[Namespace](text)
        }
        def default() : Namespace = {
            logger.info(s"Reading default namespace")
            val loader = Thread.currentThread.getContextClassLoader
            val url = loader.getResource("com/dimajix/flowman/default-namespace.yml")
            this.url(url)
        }
    }

    def read = new Reader

    def builder() = new Builder
}


class Namespace {
    @JsonProperty(value="store") private var _store: StorageSpec = _
    @JsonProperty(value="catalog") private var _catalog: CatalogSpec = _
    @JsonProperty(value="name") private var _name: String = "default"
    @JsonProperty(value="environment") private var _environment: Seq[String] = Seq()
    @JsonProperty(value="config") private var _config: Seq[String] = Seq()
    @JsonDeserialize(converter=classOf[Profile.NameResolver])
    @JsonProperty(value="profiles") private var _profiles: Map[String,Profile] = Map()
    @JsonDeserialize(converter=classOf[ConnectionSpec.NameResolver])
    @JsonProperty(value="connections") private var _connections: Map[String,ConnectionSpec] = Map()
    @JsonProperty(value="history") private var _history : HistorySpec = new NullHistorySpec()
    @JsonProperty(value="metrics") private var _metrics : MetricSinkSpec = new NullMetricSinkSpec()
    @JsonProperty(value="plugins") private var _plugins: Seq[String] = Seq()

    def name : String = _name

    def config : Map[String,String] = splitSettings(_config).toMap
    def environment : Map[String,String] = splitSettings(_environment).toMap
    def plugins : Seq[String] = _plugins

    def profiles : Map[String,Profile] = _profiles
    def connections : Map[String,ConnectionSpec] = _connections
    def storage : StorageSpec = _store
    def catalog : CatalogSpec = _catalog
    def history : HistorySpec = _history
    def metrics : MetricSinkSpec = _metrics
}
