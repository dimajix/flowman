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
import com.dimajix.flowman.documentation.CheckCollector
import com.dimajix.flowman.documentation.LineageCollector
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.Spec


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "mappings", value = classOf[MappingCollectorSpec]),
    new JsonSubTypes.Type(name = "relations", value = classOf[RelationCollectorSpec]),
    new JsonSubTypes.Type(name = "targets", value = classOf[TargetCollectorSpec]),
    new JsonSubTypes.Type(name = "lineage", value = classOf[LineageCollectorSpec]),
    new JsonSubTypes.Type(name = "checks", value = classOf[CheckCollectorSpec])
))
abstract class CollectorSpec extends Spec[Collector] {
    override def instantiate(context: Context, properties:Option[Collector.Properties] = None): Collector
}

final class MappingCollectorSpec extends CollectorSpec {
    override def instantiate(context: Context, properties:Option[Collector.Properties] = None): MappingCollector = {
        new MappingCollector()
    }
}

final class RelationCollectorSpec extends CollectorSpec {
    override def instantiate(context: Context, properties:Option[Collector.Properties] = None): RelationCollector = {
        new RelationCollector()
    }
}

final class TargetCollectorSpec extends CollectorSpec {
    override def instantiate(context: Context, properties:Option[Collector.Properties] = None): TargetCollector = {
        new TargetCollector()
    }
}

final class LineageCollectorSpec extends CollectorSpec {
    override def instantiate(context: Context, properties:Option[Collector.Properties] = None): LineageCollector = {
        new LineageCollector()
    }
}

final class CheckCollectorSpec extends CollectorSpec {
    override def instantiate(context: Context, properties:Option[Collector.Properties] = None): CheckCollector = {
        new CheckCollector()
    }
}
