/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.spec.connection.ConnectionSpec
import com.dimajix.flowman.spec.mapping.MappingSpec
import com.dimajix.flowman.spec.job.JobSpec
import com.dimajix.flowman.spec.relation.RelationSpec
import com.dimajix.flowman.spec.target.TargetSpec
import com.dimajix.flowman.spec.template.TemplateSpec
import com.dimajix.flowman.spec.test.TestSpec


final class ModuleSpec {
    @JsonProperty(value="environment") private var environment: Seq[String] = Seq()
    @JsonProperty(value="config") private var config: Seq[String] = Seq()
    @JsonDeserialize(converter=classOf[ProfileSpec.NameResolver])
    @JsonProperty(value="profiles") private var profiles: Map[String,ProfileSpec] = Map()
    @JsonDeserialize(converter=classOf[ConnectionSpec.NameResolver])
    @JsonProperty(value="connections") private var connections: Map[String,ConnectionSpec] = Map()
    @JsonDeserialize(converter=classOf[RelationSpec.NameResolver])
    @JsonProperty(value="relations") private var relations: Map[String,RelationSpec] = Map()
    @JsonDeserialize(converter=classOf[MappingSpec.NameResolver])
    @JsonProperty(value="mappings") private var mappings: Map[String,MappingSpec] = Map()
    @JsonDeserialize(converter=classOf[TargetSpec.NameResolver])
    @JsonProperty(value="targets") private var targets: Map[String,TargetSpec] = Map()
    @JsonDeserialize(converter=classOf[JobSpec.NameResolver])
    @JsonProperty(value="jobs") private var jobs: Map[String,JobSpec] = Map()
    @JsonDeserialize(converter=classOf[TestSpec.NameResolver])
    @JsonProperty(value="tests") private var tests: Map[String,TestSpec] = Map()
    @JsonDeserialize(converter=classOf[TemplateSpec.NameResolver])
    @JsonProperty(value="templates") private var templates: Map[String,TemplateSpec] = Map()

    def instantiate() : Module = {
        Module(
            environment = splitSettings(environment).toMap,
            config = splitSettings(config).toMap,
            profiles = profiles.map { case(k,v) => k -> v.instantiate() },
            connections = connections,
            relations = relations,
            mappings = mappings,
            targets = targets,
            jobs = jobs,
            tests = tests,
            templates = templates
        )
    }
}
