/*
 * Copyright (C) 2018 The Flowman Authors
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

import scala.collection.mutable

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.sql.DataFrame

import com.dimajix.common.IdentityHashMap
import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.ScopeContext
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.types.StructType


final case class UnitMapping(
    instanceProperties:Mapping.Properties,
    mappings:Map[String,Prototype[Mapping]],
    environment:Map[String,String]
) extends BaseMapping {
    private val unitContext = ScopeContext.builder(context)
        .withEnvironment(environment)
        .withMappings(mappings)
        .build()
    private val mappingInstances = mappings.keys.toSeq
        .map(name => (name,unitContext.getMapping(MappingIdentifier(name))))
        .toMap

    /**
      * Returns a list of physical resources required by this mapping. This list will only be non-empty for mappings
      * which actually read from physical data.
      * @return
      */
    override def requires : Set[ResourceIdentifier] = {
        mappingInstances
            .values
            .flatMap(_.requires)
            .toSet
    }

    /**
      * Return all outputs provided by this unit
      * @return
      */
    override def outputs: Set[String] = {
        mappingInstances
            .filter(_._2.outputs.contains("main"))
            .keySet
    }

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs: Set[MappingOutputIdentifier] = {
        // For all mappings, find only external dependencies.
        val ownMappings = mappingInstances.keySet
        mappingInstances.values
            .filter(_.outputs.contains("main"))
            .flatMap(_.inputs)
            .filter(dep => dep.project.nonEmpty || !ownMappings.contains(dep.name))
            .toSet
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param execution
      * @param input
      * @return
      */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = {
        require(execution != null)
        require(input != null)

        val cache = IdentityHashMap[Mapping,Map[String,DataFrame]]()
        def instantiate(mapping:Mapping) : Map[String,DataFrame] = {
            val deps = dependencies(mapping)
            mapping.execute(execution, deps)
        }
        def instantiate2(context:Context, id:MappingOutputIdentifier) = {
            val mapping = context.getMapping(id.mapping)
            val dfs = cache.getOrElseUpdate(mapping, instantiate(mapping))
            dfs(id.output)
        }
        def dependencies(mapping:Mapping) ={
            mapping.inputs
                .map(dep => (dep, input.getOrElse(dep, instantiate2(mapping.context, dep))))
                .toMap
        }

        mappingInstances
            .filter(_._2.outputs.contains("main"))
            .map{ case (id,mapping) => id -> instantiate(mapping)("main") }
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      *
      * @param input
      * @return
      */
    override def describe(execution:Execution, input: Map[MappingOutputIdentifier, StructType]): Map[String, StructType] = {
        require(execution != null)
        require(input != null)

        val cache = IdentityHashMap[Mapping, Map[String,StructType]]()
        def describe(mapping:Mapping) : Map[String,StructType] = {
            val deps = dependencies(mapping)
            mapping.describe(execution, deps)
        }
        def describe2(context:Context, id:MappingOutputIdentifier) : StructType = {
            val mapping = context.getMapping(id.mapping)
            val map = cache.getOrElseUpdate(mapping, describe(mapping))
            map(id.output)
        }
        def dependencies(mapping:Mapping) ={
            mapping.inputs
                .map(dep => dep -> input.getOrElse(dep, describe2(mapping.context, dep)))
                .toMap
        }

        val schema = mappingInstances
            .filter(_._2.outputs.contains("main"))
            .map { case(id,mapping) => id -> describe(mapping)("main") }

        applyDocumentation(schema)
    }
}



class UnitMappingSpec extends MappingSpec {
    @JsonProperty(value = "environment", required = true) private var environment:Seq[String] = Seq()
    @JsonDeserialize(converter=classOf[MappingSpec.NameResolver])
    @JsonProperty(value = "mappings", required = true) private var mappings:Map[String,MappingSpec] = Map()

    /**
      * Creates an instance of this specification and performs the interpolation of all variables
      *
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): UnitMapping = {
        UnitMapping(
            instanceProperties(context, properties),
            mappings,
            splitSettings(environment).toMap
        )
    }
}
