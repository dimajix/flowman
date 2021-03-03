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

package com.dimajix.flowman.spec.target

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.common.Trilean
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.ScopeContext
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.spec.splitSettings


class TemplateTarget(
    override val instanceProperties:Target.Properties,
    val target:TargetIdentifier,
    val environment:Map[String,String]
) extends BaseTarget {
    private val templateContext = ScopeContext.builder(context)
        .withEnvironment(environment)
        .build()
    private val targetInstance = {
        project.get.targets(target.name).instantiate(templateContext)
    }

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = targetInstance.phases

    /**
     * Returns an explicit user defined list of targets to be executed after this target. I.e. this
     * target needs to be executed before all other targets in this list.
     *
     * @return
     */
    override def before: Seq[TargetIdentifier] = {
        super.before ++ targetInstance.before
    }

    /**
     * Returns an explicit user defined list of targets to be executed before this target I.e. this
     * * target needs to be executed after all other targets in this list.
     *
     * @return
     */
    override def after: Seq[TargetIdentifier] = {
        super.after ++ targetInstance.after
    }

    /**
     * Returns a list of physical resources produced by this target
     *
     * @return
     */
    override def provides(phase: Phase): Set[ResourceIdentifier] = {
        targetInstance.provides(phase)
    }

    /**
     * Returns a list of physical resources required by this target
     *
     * @return
     */
    override def requires(phase: Phase): Set[ResourceIdentifier] = {
        targetInstance.requires(phase)
    }

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     *
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase): Trilean = {
        targetInstance.dirty(execution, phase)
    }

    /**
     * Executes a specific phase of this target
     *
     * @param execution
     * @param phase
     */
    override def execute(execution: Execution, phase: Phase): Unit = {
        targetInstance.execute(execution, phase)
    }
}




class TemplateTargetSpec extends TargetSpec {
    @JsonProperty(value = "target", required = true) private var target:String = _
    @JsonProperty(value = "environment", required = true) private var environment:Seq[String] = Seq()

    /**
     * Creates an instance of this specification and performs the interpolation of all variables
     *
     * @param context
     * @return
     */
    override def instantiate(context: Context): TemplateTarget = {
        new TemplateTarget(
            instanceProperties(context),
            TargetIdentifier(context.evaluate(target)),
            splitSettings(environment).toMap
        )
    }
}
