/*
 * Copyright 2022 Kaya Kupferschmidt
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
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.documentation.Collector
import com.dimajix.flowman.documentation.Documenter
import com.dimajix.flowman.documentation.Generator
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.documentation.CollectorSpec
import com.dimajix.flowman.spec.documentation.DocumenterLoader
import com.dimajix.flowman.spec.documentation.GeneratorSpec


case class DocumentTarget(
    instanceProperties:Target.Properties,
    collectors:Seq[Collector] = Seq(),
    generators:Seq[Generator] = Seq()
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(getClass)

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.VERIFY)

    /**
     * Returns a list of physical resources required by this target
     * @return
     */
    override def requires(phase: Phase) : Set[ResourceIdentifier] = Set()

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase) : Trilean = {
        phase match {
            case Phase.VERIFY => Yes
            case _ => No
        }
    }

    /**
     * Build the documentation target
     *
     * @param execution
     */
    override def verify(execution:Execution) : Unit = {
        require(execution != null)

        project match {
            case Some(project) =>
                document(execution, project)
            case None =>
                logger.warn("Cannot generate documentation without project")
        }
    }

    private def document(execution:Execution, project:Project) : Unit = {
        val documenter =
            if (collectors.isEmpty && generators.isEmpty) {
                DocumenterLoader.load(context, project)
            }
            else {
                Documenter(collectors, generators)
            }

        documenter.execute(context, execution, project)
    }
}


class DocumentTargetSpec extends TargetSpec {
    @JsonProperty(value="collectors") private var collectors: Seq[CollectorSpec] = Seq()
    @JsonProperty(value="generators") private var generators: Seq[GeneratorSpec] = Seq()

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): DocumentTarget = {
        DocumentTarget(
            instanceProperties(context, properties),
            collectors.map(_.instantiate(context)),
            generators.map(_.instantiate(context))
        )
    }
}
