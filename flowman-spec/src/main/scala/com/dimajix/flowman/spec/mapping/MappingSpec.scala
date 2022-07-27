/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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
import com.fasterxml.jackson.annotation.JsonProperty.Access
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.annotation.JsonTypeResolver
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.apache.spark.storage.StorageLevel

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.annotation.MappingType
import com.dimajix.flowman.spec.documentation.MappingDocSpec
import com.dimajix.flowman.spec.template.CustomTypeResolverBuilder
import com.dimajix.flowman.spec.template.MappingTemplateInstanceSpec
import com.dimajix.flowman.spi.ClassAnnotationHandler


object MappingSpec extends TypeRegistry[MappingSpec] {
    final class NameResolver extends NamedSpec.NameResolver[MappingSpec]
}

/**
  * Interface class for specifying a transformation (mapping)
  */
@JsonTypeResolver(classOf[CustomTypeResolverBuilder])
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "aggregate", value = classOf[AggregateMappingSpec]),
    new JsonSubTypes.Type(name = "alias", value = classOf[AliasMappingSpec]),
    new JsonSubTypes.Type(name = "assemble", value = classOf[AssembleMappingSpec]),
    new JsonSubTypes.Type(name = "case", value = classOf[CaseMappingSpec]),
    new JsonSubTypes.Type(name = "cast", value = classOf[CastMappingSpec]),
    new JsonSubTypes.Type(name = "coalesce", value = classOf[CoalesceMappingSpec]),
    new JsonSubTypes.Type(name = "conform", value = classOf[ConformMappingSpec]),
    new JsonSubTypes.Type(name = "deduplicate", value = classOf[DeduplicateMappingSpec]),
    new JsonSubTypes.Type(name = "distinct", value = classOf[DistinctMappingSpec]),
    new JsonSubTypes.Type(name = "drop", value = classOf[DropMappingSpec]),
    new JsonSubTypes.Type(name = "earliest", value = classOf[EarliestMappingSpec]),
    new JsonSubTypes.Type(name = "empty", value = classOf[EmptyMappingSpec]),
    new JsonSubTypes.Type(name = "explode", value = classOf[ExplodeMappingSpec]),
    new JsonSubTypes.Type(name = "extend", value = classOf[ExtendMappingSpec]),
    new JsonSubTypes.Type(name = "extractJson", value = classOf[ExtractJsonMappingSpec]),
    new JsonSubTypes.Type(name = "filter", value = classOf[FilterMappingSpec]),
    new JsonSubTypes.Type(name = "flatten", value = classOf[FlattenMappingSpec]),
    new JsonSubTypes.Type(name = "groupedAggregate", value = classOf[GroupedAggregateMappingSpec]),
    new JsonSubTypes.Type(name = "historize", value = classOf[HistorizeMappingSpec]),
    new JsonSubTypes.Type(name = "join", value = classOf[JoinMappingSpec]),
    new JsonSubTypes.Type(name = "latest", value = classOf[LatestMappingSpec]),
    new JsonSubTypes.Type(name = "mock", value = classOf[MockMappingSpec]),
    new JsonSubTypes.Type(name = "project", value = classOf[ProjectMappingSpec]),
    new JsonSubTypes.Type(name = "provided", value = classOf[ProvidedMappingSpec]),
    new JsonSubTypes.Type(name = "relation", value = classOf[RelationMappingSpec]),
    new JsonSubTypes.Type(name = "readHive", value = classOf[ReadHiveMappingSpec]),
    new JsonSubTypes.Type(name = "stream", value = classOf[StreamMappingSpec]),
    new JsonSubTypes.Type(name = "rebalance", value = classOf[RebalanceMappingSpec]),
    new JsonSubTypes.Type(name = "recursiveSql", value = classOf[RecursiveSqlMappingSpec]),
    new JsonSubTypes.Type(name = "repartition", value = classOf[RepartitionMappingSpec]),
    new JsonSubTypes.Type(name = "schema", value = classOf[SchemaMappingSpec]),
    new JsonSubTypes.Type(name = "select", value = classOf[SelectMappingSpec]),
    new JsonSubTypes.Type(name = "sort", value = classOf[SortMappingSpec]),
    new JsonSubTypes.Type(name = "sql", value = classOf[SqlMappingSpec]),
    new JsonSubTypes.Type(name = "stack", value = classOf[StackMappingSpec]),
    new JsonSubTypes.Type(name = "template", value = classOf[TemplateMappingSpec]),
    new JsonSubTypes.Type(name = "transitiveChildren", value = classOf[TransitiveChildrenMappingSpec]),
    new JsonSubTypes.Type(name = "union", value = classOf[UnionMappingSpec]),
    new JsonSubTypes.Type(name = "unit", value = classOf[UnitMappingSpec]),
    new JsonSubTypes.Type(name = "unpackJson", value = classOf[UnpackJsonMappingSpec]),
    new JsonSubTypes.Type(name = "upsert", value = classOf[UpsertMappingSpec]),
    new JsonSubTypes.Type(name = "values", value = classOf[ValuesMappingSpec]),
    new JsonSubTypes.Type(name = "template/*", value = classOf[MappingTemplateInstanceSpec])
))
abstract class MappingSpec extends NamedSpec[Mapping] {
    @JsonProperty(value="kind", access=Access.WRITE_ONLY, required = true) protected var kind: String = _
    @JsonSchemaInject(json="""{"type": [ "boolean", "string" ]}""")
    @JsonProperty(value="broadcast", required = false) protected var broadcast:String = "false"
    @JsonSchemaInject(json="""{"type": [ "boolean", "string" ]}""")
    @JsonProperty(value="checkpoint", required = false) protected var checkpoint:String = "false"
    @JsonProperty(value="cache", required = false) protected var cache:String = "NONE"
    @JsonProperty(value="documentation", required = false) private var documentation: Option[MappingDocSpec] = None

    /**
      * Creates an instance of this specification and performs the interpolation of all variables
      * @param context
      * @return
      */
    override def instantiate(context:Context, properties:Option[Mapping.Properties] = None) : Mapping

    /**
      * Returns a set of common properties
      * @param context
      * @return
      */
    override protected def instanceProperties(context:Context, properties:Option[Mapping.Properties]) : Mapping.Properties = {
        require(context != null)
        val name = context.evaluate(this.name)
        val props = Mapping.Properties(
            context,
            metadata.map(_.instantiate(context, name, Category.MAPPING, kind)).getOrElse(Metadata(context, name, Category.MAPPING, kind)),
            context.evaluate(broadcast).toBoolean,
            context.evaluate(checkpoint).toBoolean,
            StorageLevel.fromString(context.evaluate(cache)),
            documentation.map(_.instantiate(context))
        )
        properties.map(p => props.merge(p)).getOrElse(props)
    }
}


class MappingSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[MappingType]

    override def register(clazz: Class[_]): Unit =
        MappingSpec.register(clazz.getAnnotation(classOf[MappingType]).kind(), clazz.asInstanceOf[Class[_ <: MappingSpec]])
}
