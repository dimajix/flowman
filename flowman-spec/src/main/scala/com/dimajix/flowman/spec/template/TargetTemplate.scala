/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

import com.dimajix.common.Trilean
import com.dimajix.flowman.documentation.TargetDoc
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.AbstractInstance
import com.dimajix.flowman.model.BaseTemplate
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.spec.target.TargetSpec


case class TargetTemplate(
    instanceProperties: Template.Properties,
    parameters: Seq[Template.Parameter],
    spec:Prototype[Target]
) extends BaseTemplate[Target] with com.dimajix.flowman.model.TargetTemplate {
    override protected def instantiateInternal(context: Context, name: String): Target = {
        spec match {
            case spec:TargetSpec =>
                // Temporarily set spec name. Project and namespace are already correctly provided by the context.
                synchronized {
                    val oldName = spec.name
                    spec.name = name
                    val instance = spec.instantiate(context)
                    spec.name = oldName
                    instance
                }
            case spec => spec.instantiate(context)
        }
    }
}

class TargetTemplateSpec extends TemplateSpec {
    @JsonProperty(value="template", required=true) private var spec:TargetSpec = _

    override def instantiate(context: Context): TargetTemplate = {
        TargetTemplate(
            instanceProperties(context),
            parameters.map(_.instantiate(context)),
            spec
        )
    }
}


final case class TargetTemplateInstance(
    instanceProperties: Target.Properties,
    instance: Target
) extends AbstractInstance with Target {
    /**
     * Returns the kind of the resource
     *
     * @return
     */
    override def kind: String = instance.kind

    /**
     * Returns an identifier for this target
     *
     * @return
     */
    override def identifier: TargetIdentifier = instanceProperties.identifier

    /**
     * Returns a description of the build target
     *
     * @return
     */
    override def description: Option[String] = instanceProperties.description.orElse(instance.description)

    /**
     * Returns a (static) documentation of this target
     *
     * @return
     */
    override def documentation: Option[TargetDoc] = {
        instanceProperties.documentation
            .map(_.merge(instance.documentation))
            .orElse(instance.documentation)
            .map(_.copy(target=Some(this)))
    }

    /**
     * Returns an instance representing this target with the context
     *
     * @return
     */
    override def digest(phase: Phase): TargetDigest = {
        val digest = instance.digest(phase)
        digest.copy(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name
        )
    }

    /**
     * Returns an explicit user defined list of targets to be executed after this target. I.e. this
     * target needs to be executed before all other targets in this list.
     *
     * @return
     */
    override def before: Seq[TargetIdentifier] = (instanceProperties.before ++ instance.before).distinct

    /**
     * Returns an explicit user defined list of targets to be executed before this target I.e. this
     * * target needs to be executed after all other targets in this list.
     *
     * @return
     */
    override def after: Seq[TargetIdentifier] = (instanceProperties.after ++ instance.after).distinct

    /**
     * Returns all phases which are implemented by this target in the execute method
     *
     * @return
     */
    override def phases: Set[Phase] = instance.phases

    /**
     * Returns a list of physical resources produced by this target
     *
     * @return
     */
    override def provides(phase: Phase): Set[ResourceIdentifier] = instance.provides(phase)

    /**
     * Returns a list of physical resources required by this target
     *
     * @return
     */
    override def requires(phase: Phase): Set[ResourceIdentifier] = instance.requires(phase)

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     *
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase): Trilean = instance.dirty(execution, phase)

    /**
     * Executes a specific phase of this target. This method is should not throw a non-fatal exception, instead it
     * should wrap any exception in the TargetResult
     *
     * @param execution
     * @param phase
     */
    override def execute(execution: Execution, phase: Phase): TargetResult = instance.execute(execution, phase)

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    override def link(linker: Linker, phase: Phase): Unit = instance.link(linker, phase)
}


class TargetTemplateInstanceSpec extends TargetSpec {
    @JsonIgnore
    private[spec] var args:Map[String,String] = Map()

    @JsonAnySetter
    private def setArg(name:String, value:String) : Unit = {
        args = args.updated(name, value)
    }

    override def instantiate(context: Context): Target = {
        // get template name from member "kind"
        // Lookup template in context
        val identifier = TemplateIdentifier(kind.stripPrefix("template/"))
        val template = context.getTemplate(identifier).asInstanceOf[TargetTemplate]

        // parse args
        val parsedArgs = template.arguments(context.evaluate(args))
        val instance = template.instantiate(context, name, parsedArgs)

        TargetTemplateInstance(
            instanceProperties(context),
            instance
        )
    }
}
