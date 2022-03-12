/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.template

import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import com.dimajix.flowman.documentation.MappingDoc
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.AbstractInstance
import com.dimajix.flowman.model.BaseTemplate
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.spec.mapping.MappingSpec
import com.dimajix.flowman.types.StructType


case class MappingTemplate(
    instanceProperties: Template.Properties,
    parameters: Seq[Template.Parameter],
    spec:Prototype[Mapping]
) extends BaseTemplate[Mapping] with com.dimajix.flowman.model.MappingTemplate {
    override protected def instantiateInternal(context: Context, name: String): Mapping = {
        spec.instantiate(context)
    }
}

class MappingTemplateSpec extends TemplateSpec {
    @JsonProperty(value="template", required=true) private var spec:MappingSpec = _

    override def instantiate(context: Context): MappingTemplate = {
        MappingTemplate(
            instanceProperties(context),
            parameters.map(_.instantiate(context)),
            spec
        )
    }
}


final case class MappingTemplateInstance(
    instanceProperties: Mapping.Properties,
    instance: Mapping
) extends AbstractInstance with Mapping {
    /**
     * Returns the kind of the resource
     *
     * @return
     */
    override def kind: String = instance.kind

    /**
     * Returns an identifier for this mapping
     *
     * @return
     */
    override def identifier: MappingIdentifier = instanceProperties.identifier

    /**
     * Returns a (static) documentation of this mapping
     *
     * @return
     */
    override def documentation: Option[MappingDoc] = {
        instanceProperties.documentation
            .map(_.merge(instance.documentation))
            .orElse(instance.documentation)
            .map(_.copy(mapping=Some(this)))
    }

    /**
     * This method should return true, if the resulting dataframe should be broadcast for map-side joins
     *
     * @return
     */
    override def broadcast: Boolean = instanceProperties.broadcast || instance.broadcast

    /**
     * This method should return true, if the resulting dataframe should be checkpointed
     *
     * @return
     */
    override def checkpoint: Boolean = instanceProperties.checkpoint || instance.checkpoint

    /**
     * Returns the desired storage level. Default should be StorageLevel.NONE
     *
     * @return
     */
    override def cache: StorageLevel = {
        if (instanceProperties.cache != StorageLevel.NONE)
            instanceProperties.cache
        else
            instance.cache
    }

    /**
     * Returns a list of physical resources required by this mapping. This list will only be non-empty for mappings
     * which actually read from physical data.
     *
     * @return
     */
    override def requires: Set[ResourceIdentifier] = instance.requires

    /**
     * Returns the dependencies (i.e. names of tables in the Dataflow model)
     *
     * @return
     */
    override def inputs: Set[MappingOutputIdentifier] = instance.inputs

    /**
     * Lists all outputs of this mapping. Every mapping should have one "main" output, which is the default output
     * implicitly used when no output is specified. But eventually, the "main" output is not mandatory, but
     * recommended.
     *
     * @return
     */
    override def outputs: Set[String] = instance.outputs

    /**
     * Creates an output identifier for the primary output
     *
     * @return
     */
    override def output: MappingOutputIdentifier = instance.output

    /**
     * Creates an output identifier for the specified output name
     *
     * @param name
     * @return
     */
    override def output(name: String): MappingOutputIdentifier = instance.output(name)

    /**
     * Executes this Mapping and returns a corresponding map of DataFrames per output. The map should contain
     * one entry for each declared output in [[outputs]]. If it contains an additional entry called `cache`, then
     * this [[DataFrame]] will be cached instead of all outputs. The `cache` DataFrame may even well be some
     * internal [[DataFrame]] which is not listed in [[outputs]].
     *
     * @param execution
     * @param input
     * @return
     */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = instance.execute(execution, input)

    /**
     * Returns the schema as produced by this mapping, relative to the given input schema. The method should
     * return one entry for each entry declared in [[outputs]].
     *
     * @param input
     * @return
     */
    override def describe(execution: Execution, input: Map[MappingOutputIdentifier, StructType]): Map[String, StructType] = instance.describe(execution, input)

    /**
     * Returns the schema as produced by this mapping, relative to the given input schema
     *
     * @param input
     * @return
     */
    override def describe(execution: Execution, input: Map[MappingOutputIdentifier, StructType], output: String): StructType = instance.describe(execution, input, output)

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    override def link(linker: Linker): Unit = instance.link(linker)
}


class MappingTemplateInstanceSpec extends MappingSpec {
    @JsonIgnore
    private[spec] var args: Map[String, String] = Map()

    @JsonAnySetter
    private def setArg(name: String, value: String): Unit = {
        args = args.updated(name, value)
    }

    override def instantiate(context: Context): Mapping = {
        // get template name from member "kind"
        // Lookup template in context
        val identifier = TemplateIdentifier(kind.stripPrefix("template/"))
        val template = context.getTemplate(identifier).asInstanceOf[MappingTemplate]

        // parse args
        val parsedArgs = template.arguments(context.evaluate(args))
        val instance = template.instantiate(context, name, parsedArgs)

        MappingTemplateInstance(
            instanceProperties(context),
            instance
        )
    }
}
