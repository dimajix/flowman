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

package com.dimajix.flowman.spec.target

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_OUTPUT_MODE
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_PARALLELISM
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_REBALANCE
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.RelationReference
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.spec.relation.RelationReferenceSpec
import com.dimajix.flowman.types.ArrayValue
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.RangeValue
import com.dimajix.flowman.types.SingleValue


object TruncateTarget {
    def apply(context: Context, relation: RelationIdentifier) : TruncateTarget = {
        new TruncateTarget(
            Target.Properties(context, relation.name, "relation"),
            RelationReference(context, relation),
            Map()
        )
    }
    def apply(context: Context, relation: RelationIdentifier, partitions:Map[String,FieldValue]) : TruncateTarget = {
        new TruncateTarget(
            Target.Properties(context, relation.name, "relation"),
            RelationReference(context, relation),
            partitions
        )
    }
}
case class TruncateTarget(
    instanceProperties: Target.Properties,
    relation: Reference[Relation],
    partitions:Map[String,FieldValue] = Map()
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[RelationTarget])

    /**
     * Returns an instance representing this target with the context
     * @return
     */
    override def digest(phase:Phase) : TargetDigest = {
        TargetDigest(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            phase,
            // TODO: Maybe here should be a partition or a list of partitions....
            Map()
        )
    }

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = {
        Set(Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE)
    }

    /**
     * Returns a list of physical resources produced by this target
     * @return
     */
    override def provides(phase: Phase) : Set[ResourceIdentifier] = {
        phase match {
            case Phase.BUILD|Phase.TRUNCATE =>
                val rel = relation.value
                rel.provides ++ rel.resources(partitions)
            case _ => Set()
        }
    }

    /**
     * Returns a list of physical resources required by this target
     * @return
     */
    override def requires(phase: Phase) : Set[ResourceIdentifier] = {
        phase match {
            case Phase.BUILD|Phase.TRUNCATE =>
                val rel = relation.value
                rel.provides ++ rel.requires
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
        phase match {
            case Phase.BUILD|Phase.TRUNCATE =>
                val rel = relation.value
                resolvedPartitions(rel).foldLeft(No:Trilean)((l,p) => l || rel.loaded(execution, p))
            case Phase.VERIFY => Yes
            case _ => No
        }
    }

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    override def link(linker: Linker, phase:Phase): Unit = {
        phase match {
            case Phase.BUILD|Phase.TRUNCATE =>
                val rel = relation.value
                resolvedPartitions(rel).foreach(p => linker.write(rel, p))
            case _ =>
        }
    }

    /**
     * Builds the target using the given input tables
     *
     * @param execution
     */
    override def build(execution:Execution) : Unit = {
        require(execution != null)

        val rel = relation.value
        rel.truncate(execution, partitions)
    }

    /**
     * Performs a verification of the build step or possibly other checks.
     *
     * @param execution
     */
    override def verify(execution: Execution) : Unit = {
        require(execution != null)

        val rel = relation.value
        resolvedPartitions(rel)
            .find(p => rel.loaded(execution, p) == Yes)
            .foreach { partition =>
                if (partition.isEmpty)
                    logger.error(s"Verification of target '$identifier' failed - relation '$relation' not empty")
                else
                    logger.error(s"Verification of target '$identifier' failed - partition $partition of relation '$relation' exists")
                throw new VerificationFailedException(identifier)
            }
    }

    /**
     * Builds the target using the given input tables
     *
     * @param execution
     */
    override def truncate(execution:Execution) : Unit = {
        require(execution != null)

        val rel = relation.value
        rel.truncate(execution, partitions)
    }

    private def resolvedPartitions(relation:Relation) : Iterable[Map[String,SingleValue]] = {
        if (this.partitions.isEmpty) {
            Seq(Map())
        }
        else {
            PartitionSchema(relation.partitions)
                .interpolate(this.partitions)
                .map(p => p.toMap.map { case (k, v) => k -> SingleValue(v.toString) })
        }
    }
}


class TruncateTargetSpec extends TargetSpec {
    @JsonProperty(value="relation", required=true) private var relation:RelationReferenceSpec = _
    @JsonProperty(value = "partitions", required=false) private var partitions:Map[String,FieldValue] = Map()

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context): TruncateTarget = {
        val partitions= this.partitions.mapValues {
            case v: SingleValue => SingleValue(context.evaluate(v.value))
            case v: ArrayValue => ArrayValue(v.values.map(context.evaluate))
            case v: RangeValue => RangeValue(context.evaluate(v.start), context.evaluate(v.end), v.step.map(context.evaluate))
        }
        TruncateTarget(
            instanceProperties(context),
            relation.instantiate(context),
            partitions
        )
    }
}
