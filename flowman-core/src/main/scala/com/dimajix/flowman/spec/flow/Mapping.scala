/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.AbstractInstance
import com.dimajix.flowman.spec.Instance
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spi.TypeRegistry
import com.dimajix.flowman.types.StructType


object Mapping {
    object Properties {
        def apply(context:Context=null, name:String="", kind:String="") : Properties = {
            Properties(
                context,
                if (context != null) context.namespace else null,
                if (context != null) context.project else null,
                name,
                kind,
                Map(),
                false,
                false,
                StorageLevel.NONE
            )
        }
    }
    case class Properties(
         context: Context,
         namespace:Namespace,
         project:Project,
         name:String,
         kind:String,
         labels:Map[String,String],
         broadcast:Boolean,
         checkpoint:Boolean,
         cache:StorageLevel
    ) extends Instance.Properties
}


abstract class Mapping extends AbstractInstance {
    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: String = "mapping"

    /**
      * Returns an identifier for this mapping
      * @return
      */
    def identifier : MappingIdentifier

    /**
      * This method should return true, if the resulting dataframe should be broadcast for map-side joins
      * @return
      */
    def broadcast : Boolean

    /**
      * This method should return true, if the resulting dataframe should be checkpointed
      * @return
      */
    def checkpoint : Boolean

    /**
      * Returns the desired storage level. Default should be StorageLevel.NONE
      * @return
      */
    def cache : StorageLevel

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      * @return
      */
    def dependencies : Array[MappingIdentifier]

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    def execute(executor:Executor, input:Map[MappingIdentifier,DataFrame]) : DataFrame

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    def describe(input:Map[MappingIdentifier,StructType]) : StructType
}



object MappingSpec extends TypeRegistry[MappingSpec] {
    type NameResolver = NamedSpec.NameResolver[Mapping, MappingSpec]
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
    new JsonSubTypes.Type(name = "extend", value = classOf[ExtendMappingSpec]),
    new JsonSubTypes.Type(name = "extractJson", value = classOf[ExtractJsonMappingSpec]),
    new JsonSubTypes.Type(name = "filter", value = classOf[FilterMappingSpec]),
    new JsonSubTypes.Type(name = "flatten", value = classOf[FlattenMappingSpec]),
    new JsonSubTypes.Type(name = "join", value = classOf[JoinMappingSpec]),
    new JsonSubTypes.Type(name = "latest", value = classOf[LatestMappingSpec]),
    new JsonSubTypes.Type(name = "project", value = classOf[ProjectMappingSpec]),
    new JsonSubTypes.Type(name = "provided", value = classOf[ProvidedMappingSpec]),
    new JsonSubTypes.Type(name = "read", value = classOf[ReadRelationMappingSpec]),
    new JsonSubTypes.Type(name = "readRelation", value = classOf[ReadRelationMappingSpec]),
    new JsonSubTypes.Type(name = "readStream", value = classOf[ReadStreamMappingSpec]),
    new JsonSubTypes.Type(name = "rebalance", value = classOf[RebalanceMappingSpec]),
    new JsonSubTypes.Type(name = "repartition", value = classOf[RepartitionMappingSpec]),
    new JsonSubTypes.Type(name = "schema", value = classOf[SchemaMappingSpec]),
    new JsonSubTypes.Type(name = "select", value = classOf[SelectMappingSpec]),
    new JsonSubTypes.Type(name = "sort", value = classOf[SortMappingSpec]),
    new JsonSubTypes.Type(name = "sql", value = classOf[SqlMappingSpec]),
    new JsonSubTypes.Type(name = "union", value = classOf[UnionMappingSpec]),
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
            labels.mapValues(context.evaluate),
            context.evaluate(broadcast).toBoolean,
            context.evaluate(checkpoint).toBoolean,
            StorageLevel.fromString(context.evaluate(cache))
        )
    }
}
