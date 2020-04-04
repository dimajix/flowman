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

package com.dimajix.flowman.spec

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.spec.catalog.CatalogSpec
import com.dimajix.flowman.spec.connection.ConnectionSpec
import com.dimajix.flowman.spec.history.HistorySpec
import com.dimajix.flowman.spec.metric.MetricSinkSpec
import com.dimajix.flowman.spec.storage.StorageSpec


class NamespaceSpec {
    @JsonProperty(value="name") private var name: String = "default"
    @JsonProperty(value="environment") private var environment: Seq[String] = Seq()
    @JsonProperty(value="config") private var config: Seq[String] = Seq()
    @JsonDeserialize(converter=classOf[ProfileSpec.NameResolver])
    @JsonProperty(value="profiles") private var profiles: Map[String,ProfileSpec] = Map()
    @JsonDeserialize(converter=classOf[ConnectionSpec.NameResolver])
    @JsonProperty(value="connections") private var connections: Map[String,ConnectionSpec] = Map()
    @JsonProperty(value="store") private var store: Option[StorageSpec] = None
    @JsonProperty(value="catalog") private var catalog: Option[CatalogSpec] = None
    @JsonProperty(value="history") private var history : Option[HistorySpec] = None
    @JsonProperty(value="metrics") private var metrics : Option[MetricSinkSpec] = None
    @JsonProperty(value="plugins") private var plugins: Seq[String] = Seq()

    def instantiate() : Namespace = {
        Namespace(
            name,
            splitSettings(config).toMap,
            splitSettings(environment).toMap,
            profiles.map { case(k,v) => k -> v.instantiate() },
            connections,
            store,
            catalog,
            history,
            metrics,
            plugins
        )
    }
}

