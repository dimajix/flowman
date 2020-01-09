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
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.ScopeContext
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.types.StructType


case class TemplateMapping(
    instanceProperties:Mapping.Properties,
    mapping:MappingIdentifier,
    environment:Map[String,String],
    filter:Option[String] = None
) extends BaseMapping {
    private val templateContext = ScopeContext.builder(context)
        .withEnvironment(environment)
        .build()
    private val mappingInstance = {
        project.mappings(mapping.name).instantiate(templateContext)
    }


    /**
      * Returns a list of physical resources required by this mapping. This list will only be non-empty for mappings
      * which actually read from physical data.
      *
      * @return
      */
    override def requires: Set[ResourceIdentifier] = {
        mappingInstance.requires
    }

    /**
      * Lists all outputs of this mapping. Every mapping should have one "main" output
 *
      * @return
      */
    override def outputs : Seq[String] = {
        mappingInstance.outputs
    }

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs: Seq[MappingOutputIdentifier] = {
        mappingInstance.inputs
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor: Executor, input: Map[MappingOutputIdentifier, DataFrame]) : Map[String,DataFrame] = {
        require(executor != null)
        require(input != null)

        val result = mappingInstance.execute(executor, input)

        // Apply optional filter
        result.map { case(name,df) => name -> filter.map(df.filter).getOrElse(df) }
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(executor:Executor, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(executor != null)
        require(input != null)

        mappingInstance.describe(executor, input)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      *
      * @param input
      * @return
      */
    override def describe(executor:Executor, input: Map[MappingOutputIdentifier, StructType], output: String): StructType = {
        require(executor != null)
        require(input != null)
        require(output != null && output.nonEmpty)

        mappingInstance.describe(executor, input, output)
    }
}



class TemplateMappingSpec extends MappingSpec {
    @JsonProperty(value = "mapping", required = true) private var mapping:String = _
    @JsonProperty(value = "environment", required = true) private var environment:Seq[String] = Seq()
    @JsonProperty(value = "filter", required=false) private var filter:Option[String] = None

    /**
      * Creates an instance of this specification and performs the interpolation of all variables
      *
      * @param context
      * @return
      */
    override def instantiate(context: Context): TemplateMapping = {
        TemplateMapping(
            instanceProperties(context),
            MappingIdentifier(context.evaluate(mapping)),
            splitSettings(environment).toMap,
            context.evaluate(filter)
        )
    }
}
