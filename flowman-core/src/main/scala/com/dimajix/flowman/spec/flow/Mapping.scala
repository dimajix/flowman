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

package com.dimajix.flowman.spec.flow

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.util.StdConverter
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Resource
import com.dimajix.flowman.spi.TypeRegistry
import com.dimajix.flowman.types.StructType


object Mapping extends TypeRegistry[Mapping] {
    class NameResolver extends StdConverter[Map[String,Mapping],Map[String,Mapping]] {
        override def convert(value: Map[String,Mapping]): Map[String,Mapping] = {
            value.foreach(kv => kv._2._name = kv._1)
            value
        }
    }
}


/**
  * Interface class for specifying a transformation (mapping)
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "aggregate", value = classOf[AggregateMapping]),
    new JsonSubTypes.Type(name = "alias", value = classOf[AliasMapping]),
    new JsonSubTypes.Type(name = "assemble", value = classOf[AssembleMapping]),
    new JsonSubTypes.Type(name = "coalesce", value = classOf[CoalesceMapping]),
    new JsonSubTypes.Type(name = "conform", value = classOf[ConformMapping]),
    new JsonSubTypes.Type(name = "deduplicate", value = classOf[DeduplicateMapping]),
    new JsonSubTypes.Type(name = "distinct", value = classOf[DistinctMapping]),
    new JsonSubTypes.Type(name = "drop", value = classOf[DropMapping]),
    new JsonSubTypes.Type(name = "extend", value = classOf[ExtendMapping]),
    new JsonSubTypes.Type(name = "filter", value = classOf[FilterMapping]),
    new JsonSubTypes.Type(name = "join", value = classOf[JoinMapping]),
    new JsonSubTypes.Type(name = "extractJson", value = classOf[ExtractJsonMapping]),
    new JsonSubTypes.Type(name = "unpackJson", value = classOf[UnpackJsonMapping]),
    new JsonSubTypes.Type(name = "latest", value = classOf[LatestMapping]),
    new JsonSubTypes.Type(name = "update", value = classOf[UpdateMapping]),
    new JsonSubTypes.Type(name = "project", value = classOf[ProjectMapping]),
    new JsonSubTypes.Type(name = "provided", value = classOf[ProvidedMapping]),
    new JsonSubTypes.Type(name = "read", value = classOf[ReadRelationMapping]),
    new JsonSubTypes.Type(name = "readRelation", value = classOf[ReadRelationMapping]),
    new JsonSubTypes.Type(name = "readStream", value = classOf[ReadStreamMapping]),
    new JsonSubTypes.Type(name = "rebalance", value = classOf[RebalanceMapping]),
    new JsonSubTypes.Type(name = "repartition", value = classOf[RepartitionMapping]),
    new JsonSubTypes.Type(name = "schema", value = classOf[SchemaMapping]),
    new JsonSubTypes.Type(name = "select", value = classOf[SelectMapping]),
    new JsonSubTypes.Type(name = "sort", value = classOf[SortMapping]),
    new JsonSubTypes.Type(name = "sql", value = classOf[SqlMapping]),
    new JsonSubTypes.Type(name = "union", value = classOf[UnionMapping])
))
abstract class Mapping extends Resource {
    @JsonIgnore private var _name:String = ""

    @JsonProperty(value="kind", required = true) private var _kind: String = _
    @JsonProperty(value="labels", required=false) private var _labels:Map[String,String] = Map()

    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: String = "mapping"

    /**
      * Returns the specific kind of this resource
      * @return
      */
    final override def kind: String = _kind

    /**
      * Returns a map of user defined labels
      * @param context
      * @return
      */
    final override def labels(implicit context: Context) : Map[String,String] = _labels.mapValues(context.evaluate)

    /**
      * Returns the name of the mapping
      * @return
      */
    final override def name : String = _name

    /**
      * This method should return true, if the resulting dataframe should be broadcast for map-side joins
      * @param context
      * @return
      */
    def broadcast(implicit context: Context) : Boolean

    /**
      * This method should return true, if the resulting dataframe should be checkpointed
      * @param context
      * @return
      */
    def checkpoint(implicit context: Context) : Boolean

    /**
      * Returns the desired storage level. Default should be StorageLevel.NONE
      * @param context
      * @return
      */
    def cache(implicit context: Context) : StorageLevel

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      * @param context
      * @return
      */
    def dependencies(implicit context:Context) : Array[MappingIdentifier]

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
      * @param context
      * @param input
      * @return
      */
    def describe(context:Context, input:Map[MappingIdentifier,StructType]) : StructType
}
