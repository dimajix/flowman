/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.ScopeContext
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.types.StructType


case class UnitMapping(
    instanceProperties:Mapping.Properties,
    mappings:Map[String,MappingSpec],
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
      * Return all outputs provided by this unit
      * @return
      */
    override def outputs: Seq[String] = {
        mappingInstances
            .filter(_._2.outputs.contains("default"))
            .keys.toSeq
    }

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def dependencies: Seq[MappingOutputIdentifier] = {
        // For all mappings, find only external dependencies.
        val ownMappings = mappingInstances.keySet
        mappingInstances.values
            .filter(_.outputs.contains("default"))
            .flatMap(_.dependencies)
            .filter(dep => dep.project.nonEmpty || !ownMappings.contains(dep.name))
            .toSeq
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor: Executor, input: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = {
        def instantiate(context:Context, id:MappingOutputIdentifier) = {
            val mapping = context.getMapping(id.mapping)
            executor.instantiate(mapping, id.output)
        }
        def dependencies(mapping:Mapping) ={
            mapping.dependencies
                .map(dep => (dep, input.getOrElse(dep, instantiate(mapping.context, dep))))
                .toMap
        }

        mappingInstances
            .filter(_._2.outputs.contains("default"))
            .map{ case (id,mapping) => (id,mapping.execute(executor, dependencies(mapping))("default")) }
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      *
      * @param input
      * @return
      */
    override def describe(input: Map[MappingOutputIdentifier, StructType]): Map[String, StructType] = {
        require(input != null)

        mappingInstances
            .filter(_._2.outputs.contains("default"))
            .keys
            .map(name => (name, describe(input, name)))
            .toMap
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      *
      * @param input
      * @return
      */
    override def describe(input: Map[MappingOutputIdentifier, StructType], output:String): StructType = {
        require(input != null)
        require(output != null && output.nonEmpty)

        def describe(context:Context, id:MappingOutputIdentifier) : StructType = {
            val mapping = context.getMapping(id.mapping)
            val deps = dependencies(mapping)
            mapping.describe(deps, id.output)
        }
        def dependencies(mapping:Mapping) ={
            mapping.dependencies
                .map(dep => (dep, input.getOrElse(dep, describe(mapping.context, dep))))
                .toMap
        }

        mappingInstances
            .filter(_._2.outputs.contains("default"))
            .get(output)
            .map(mapping => mapping.describe(dependencies(mapping), "default"))
            .getOrElse(throw new NoSuchElementException(s"Cannot find output '$output' in unit mapping '$identifier'"))
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
    override def instantiate(context: Context): UnitMapping = {
        UnitMapping(
            instanceProperties(context),
            mappings,
            splitSettings(environment).toMap
        )
    }
}
