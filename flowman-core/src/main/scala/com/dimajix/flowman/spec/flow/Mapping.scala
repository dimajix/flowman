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

import scala.collection.mutable

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.util.StdConverter
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.output.Output
import com.dimajix.flowman.spi.ExtensionRegistry


object Mapping extends ExtensionRegistry[Mapping] {
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
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "aggregate", value = classOf[AggregateMapping]),
    new JsonSubTypes.Type(name = "alias", value = classOf[AliasMapping]),
    new JsonSubTypes.Type(name = "deduplicate", value = classOf[DeduplicateMapping]),
    new JsonSubTypes.Type(name = "distinct", value = classOf[DistinctMapping]),
    new JsonSubTypes.Type(name = "read", value = classOf[ReadRelationMapping]),
    new JsonSubTypes.Type(name = "read-relation", value = classOf[ReadRelationMapping]),
    new JsonSubTypes.Type(name = "read-stream", value = classOf[ReadStreamMapping]),
    new JsonSubTypes.Type(name = "repartition", value = classOf[RepartitionMapping]),
    new JsonSubTypes.Type(name = "sort", value = classOf[SortMapping]),
    new JsonSubTypes.Type(name = "extend", value = classOf[ExtendMapping]),
    new JsonSubTypes.Type(name = "filter", value = classOf[FilterMapping]),
    new JsonSubTypes.Type(name = "join", value = classOf[JoinMapping]),
    new JsonSubTypes.Type(name = "json-extract", value = classOf[ExtractJsonMapping]),
    new JsonSubTypes.Type(name = "json-unpack", value = classOf[UnpackJsonMapping]),
    new JsonSubTypes.Type(name = "project", value = classOf[ProjectMapping]),
    new JsonSubTypes.Type(name = "provided", value = classOf[ProvidedMapping]),
    new JsonSubTypes.Type(name = "select", value = classOf[SelectMapping]),
    new JsonSubTypes.Type(name = "sql", value = classOf[SqlMapping]),
    new JsonSubTypes.Type(name = "union", value = classOf[UnionMapping])
))
abstract class Mapping {
    @JsonIgnore private var _name:String = ""

    /**
      * Returns the name of the mapping
      * @return
      */
    def name : String = _name

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
}
