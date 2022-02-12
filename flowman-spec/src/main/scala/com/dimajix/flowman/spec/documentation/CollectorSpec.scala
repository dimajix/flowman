/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.documentation

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.flowman.documentation.Collector
import com.dimajix.flowman.documentation.MappingCollector
import com.dimajix.flowman.documentation.RelationCollector
import com.dimajix.flowman.documentation.TargetCollector
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.Spec


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "mappings", value = classOf[MappingCollectorSpec]),
    new JsonSubTypes.Type(name = "relations", value = classOf[RelationCollectorSpec]),
    new JsonSubTypes.Type(name = "targets", value = classOf[TargetCollectorSpec])
))
abstract class CollectorSpec extends Spec[Collector] {
    override def instantiate(context: Context): Collector
}

final class MappingCollectorSpec extends CollectorSpec {
    override def instantiate(context: Context): MappingCollector = {
        new MappingCollector()
    }
}

final class RelationCollectorSpec extends CollectorSpec {
    override def instantiate(context: Context): RelationCollector = {
        new RelationCollector()
    }
}

final class TargetCollectorSpec extends CollectorSpec {
    override def instantiate(context: Context): TargetCollector = {
        new TargetCollector()
    }
}
