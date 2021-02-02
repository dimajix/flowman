/*
 * Copyright 2019-2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.mapping

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.util.StdConverter
import org.apache.spark.storage.StorageLevel

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.annotation.MappingType
import com.dimajix.flowman.spi.ClassAnnotationHandler


object MappingSpec extends TypeRegistry[MappingSpec] {
    class NameResolver extends StdConverter[Map[String, MappingSpec], Map[String, MappingSpec]] {
        override def convert(value: Map[String, MappingSpec]): Map[String, MappingSpec] = {
            value.foreach(kv => kv._2.name = kv._1)
            value
        }
    }
}

/**
  * Interface class for specifying a transformation (mapping)
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "aggregate", value = classOf[AggregateMappingSpec]),
    new JsonSubTypes.Type(name = "alias", value = classOf[AliasMappingSpec]),
    new JsonSubTypes.Type(name = "assemble", value = classOf[AssembleMappingSpec]),
    new JsonSubTypes.Type(name = "coalesce", value = classOf[CoalesceMappingSpec]),
    new JsonSubTypes.Type(name = "conform", value = classOf[ConformMappingSpec]),
    new JsonSubTypes.Type(name = "deduplicate", value = classOf[DeduplicateMappingSpec]),
    new JsonSubTypes.Type(name = "distinct", value = classOf[DistinctMappingSpec]),
    new JsonSubTypes.Type(name = "drop", value = classOf[DropMappingSpec]),
    new JsonSubTypes.Type(name = "earliest", value = classOf[EarliestMappingSpec]),
    new JsonSubTypes.Type(name = "explode", value = classOf[ExplodeMappingSpec]),
    new JsonSubTypes.Type(name = "extend", value = classOf[ExtendMappingSpec]),
    new JsonSubTypes.Type(name = "extractJson", value = classOf[ExtractJsonMappingSpec]),
    new JsonSubTypes.Type(name = "filter", value = classOf[FilterMappingSpec]),
    new JsonSubTypes.Type(name = "flatten", value = classOf[FlattenMappingSpec]),
    new JsonSubTypes.Type(name = "historize", value = classOf[HistorizeMappingSpec]),
    new JsonSubTypes.Type(name = "join", value = classOf[JoinMappingSpec]),
    new JsonSubTypes.Type(name = "latest", value = classOf[LatestMappingSpec]),
    new JsonSubTypes.Type(name = "project", value = classOf[ProjectMappingSpec]),
    new JsonSubTypes.Type(name = "provided", value = classOf[ProvidedMappingSpec]),
    new JsonSubTypes.Type(name = "read", value = classOf[ReadRelationMappingSpec]),
    new JsonSubTypes.Type(name = "readRelation", value = classOf[ReadRelationMappingSpec]),
    new JsonSubTypes.Type(name = "readStream", value = classOf[ReadStreamMappingSpec]),
    new JsonSubTypes.Type(name = "rebalance", value = classOf[RebalanceMappingSpec]),
    new JsonSubTypes.Type(name = "recursiveSql", value = classOf[RecursiveSqlMappingSpec]),
    new JsonSubTypes.Type(name = "repartition", value = classOf[RepartitionMappingSpec]),
    new JsonSubTypes.Type(name = "schema", value = classOf[SchemaMappingSpec]),
    new JsonSubTypes.Type(name = "select", value = classOf[SelectMappingSpec]),
    new JsonSubTypes.Type(name = "sort", value = classOf[SortMappingSpec]),
    new JsonSubTypes.Type(name = "sql", value = classOf[SqlMappingSpec]),
    new JsonSubTypes.Type(name = "template", value = classOf[TemplateMappingSpec]),
    new JsonSubTypes.Type(name = "transitiveChildren", value = classOf[TransitiveChildrenMappingSpec]),
    new JsonSubTypes.Type(name = "union", value = classOf[UnionMappingSpec]),
    new JsonSubTypes.Type(name = "unit", value = classOf[UnitMappingSpec]),
    new JsonSubTypes.Type(name = "unpackJson", value = classOf[UnpackJsonMappingSpec]),
    new JsonSubTypes.Type(name = "update", value = classOf[UpdateMappingSpec])
))
abstract class MappingSpec extends NamedSpec[Mapping] {
    @JsonProperty("broadcast") protected var broadcast:String = "false"
    @JsonProperty("checkpoint") protected var checkpoint:String = "false"
    @JsonProperty("cache") protected var cache:String = "NONE"

    /**
      * Creates an instance of this specification and performs the interpolation of all variables
      * @param context
      * @return
      */
    override def instantiate(context:Context) : Mapping

    /**
      * Returns a set of common properties
      * @param context
      * @return
      */
    override protected def instanceProperties(context:Context) : Mapping.Properties = {
        require(context != null)
        Mapping.Properties(
            context,
            context.namespace,
            context.project,
            name,
            kind,
            context.evaluate(labels),
            context.evaluate(broadcast).toBoolean,
            context.evaluate(checkpoint).toBoolean,
            StorageLevel.fromString(context.evaluate(cache))
        )
    }
}


class MappingSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[MappingType]

    override def register(clazz: Class[_]): Unit =
        MappingSpec.register(clazz.getAnnotation(classOf[MappingType]).kind(), clazz.asInstanceOf[Class[_ <: MappingSpec]])
}
