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
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.ExecutionException
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.RelationReference
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.spec.relation.RelationReferenceSpec
import com.dimajix.flowman.types.SingleValue


object DropTarget {
    def apply(context: Context, relation: RelationIdentifier) : DropTarget = {
        new DropTarget(
            Target.Properties(context, relation.name, "relation"),
            RelationReference(context, relation)
        )
    }
}
case class DropTarget(
    instanceProperties: Target.Properties,
    relation: Reference[Relation]
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[RelationTarget])

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = {
        Set(Phase.CREATE, Phase.VERIFY, Phase.DESTROY)
    }

    /**
     * Returns a list of physical resources produced by this target
     * @return
     */
    override def provides(phase: Phase) : Set[ResourceIdentifier] = {
        Set()
    }

    /**
     * Returns a list of physical resources required by this target
     * @return
     */
    override def requires(phase: Phase) : Set[ResourceIdentifier] = {
        phase match {
            case Phase.CREATE|Phase.DESTROY => relation.value.provides(Operation.CREATE) ++ relation.value.requires(Operation.CREATE)
            case _ => Set()
        }
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
        val rel = relation.value

        phase match {
            case Phase.CREATE =>
                rel.exists(execution) != No
            case Phase.VERIFY => Yes
            case Phase.DESTROY =>
                rel.exists(execution) != No
            case _ => No
        }
    }

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    override def link(linker: Linker, phase:Phase): Unit = {
        phase match {
            case Phase.CREATE|Phase.DESTROY =>
                linker.write(relation, Map.empty[String,SingleValue])
            case _ =>
        }
    }

    /**
     * Drop the relation and all data contained
     *
     * @param executor
     */
    override def create(execution: Execution) : Unit = {
        require(execution != null)

        logger.info(s"Destroying relation '${relation.identifier}'")
        val rel = relation.value
        rel.destroy(execution, true)
    }

    /**
     * Verifies that the relation does not exist any more
     *
     * @param execution
     */
    override def verify(execution: Execution) : Unit = {
        require(execution != null)

        val rel = relation.value
        if (rel.exists(execution) == Yes) {
            val error = s"Verification of target '$identifier' failed - relation '${relation.identifier}' still exists"
            logger.error(error)
            throw new VerificationFailedException(identifier, new ExecutionException(error))
        }
    }

    /**
     * Destroys both the logical relation and the physical data
     * @param executor
     */
    override def destroy(execution: Execution) : Unit = {
        require(execution != null)

        logger.info(s"Destroying relation '${relation.identifier}'")
        val rel = relation.value
        rel.destroy(execution, true)
    }
}


class DropTargetSpec extends TargetSpec {
    @JsonProperty(value="relation", required=true) private var relation:RelationReferenceSpec = _

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): DropTarget = {
        DropTarget(
            instanceProperties(context, properties),
            relation.instantiate(context)
        )
    }
}
