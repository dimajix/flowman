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

package com.dimajix.flowman.spec.mapping

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.ScopeContext
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.types.StructType


case class UnitMapping(
    instanceProperties:Mapping.Properties,
    mappings:Map[String,Template[Mapping]],
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
    override def outputs: Seq[String] = {
        mappingInstances
            .filter(_._2.outputs.contains("main"))
            .keys.toSeq
    }

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs: Seq[MappingOutputIdentifier] = {
        // For all mappings, find only external dependencies.
        val ownMappings = mappingInstances.keySet
        mappingInstances.values
            .filter(_.outputs.contains("main"))
            .flatMap(_.inputs)
            .filter(dep => dep.project.nonEmpty || !ownMappings.contains(dep.name))
            .toSeq
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param execution
      * @param input
      * @return
      */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = {
        mappingInstances
            .filter(_._2.outputs.contains("main"))
            .map{ case (id,mapping) => (id,execution.instantiate(mapping, "main")) }
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

        mappingInstances
            .filter(_._2.outputs.contains("main"))
            .keys
            .map(name => name -> describe(execution, input, name))
            .toMap
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      *
      * @param input
      * @return
      */
    override def describe(execution:Execution, input: Map[MappingOutputIdentifier, StructType], output:String): StructType = {
        require(execution != null)
        require(input != null)
        require(output != null && output.nonEmpty)

        def describe(mapping:Mapping, output:String) : StructType = {
            val deps = dependencies(mapping)
            mapping.describe(execution, deps, output)
        }
        def describe2(context:Context, id:MappingOutputIdentifier) : StructType = {
            val mapping = context.getMapping(id.mapping)
            describe(mapping, id.output)
        }
        def dependencies(mapping:Mapping) ={
            mapping.inputs
                .map(dep => dep -> input.getOrElse(dep, describe2(mapping.context, dep)))
                .toMap
        }

        mappingInstances
            .filter(_._2.outputs.contains("main"))
            .get(output)
            .map(mapping => describe(mapping, "main"))
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
