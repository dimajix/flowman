/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

import java.time.Instant

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.slf4j.LoggerFactory

import com.dimajix.common.MapIgnoreCase
import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.common.Yes
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_RELATION_MIGRATION_POLICY
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_RELATION_MIGRATION_STRATEGY
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_BUILD_POLICY
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_OUTPUT_MODE
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_PARALLELISM
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_REBALANCE
import com.dimajix.flowman.execution.BuildPolicy
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.ExecutionException
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.RelationReference
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.model.VerifyPolicy
import com.dimajix.flowman.spec.relation.IdentifierRelationReferenceSpec
import com.dimajix.flowman.spec.relation.RelationReferenceSpec
import com.dimajix.flowman.types.SingleValue


object RelationTarget {
    def apply(context: Context, relation: RelationIdentifier) : RelationTarget = {
        val conf = context.flowmanConf
        new RelationTarget(
            Target.Properties(context, relation.name, "relation"),
            RelationReference(context, relation),
            MappingOutputIdentifier.empty,
            OutputMode.ofString(conf.getConf(DEFAULT_TARGET_OUTPUT_MODE)),
            Map(),
            conf.getConf(DEFAULT_TARGET_PARALLELISM),
            conf.getConf(DEFAULT_TARGET_REBALANCE),
            BuildPolicy.ofString(conf.getConf(DEFAULT_TARGET_BUILD_POLICY))
        )
    }
    def apply(context: Context, relation: RelationIdentifier, mapping: MappingOutputIdentifier) : RelationTarget = {
        val conf = context.flowmanConf
        new RelationTarget(
            Target.Properties(context, relation.name, "relation"),
            RelationReference(context, relation),
            mapping,
            OutputMode.ofString(conf.getConf(DEFAULT_TARGET_OUTPUT_MODE)),
            Map(),
            conf.getConf(DEFAULT_TARGET_PARALLELISM),
            conf.getConf(DEFAULT_TARGET_REBALANCE),
            BuildPolicy.ofString(conf.getConf(DEFAULT_TARGET_BUILD_POLICY))
        )
    }
    def apply(props:Target.Properties, relation: RelationIdentifier, mapping: MappingOutputIdentifier, partition: Map[String,String]) : RelationTarget = {
        val context = props.context
        val conf = context.flowmanConf
        new RelationTarget(
            props.copy(metadata=props.metadata.copy(kind="relation")),
            RelationReference(context, relation),
            mapping,
            OutputMode.ofString(conf.getConf(DEFAULT_TARGET_OUTPUT_MODE)),
            partition,
            conf.getConf(DEFAULT_TARGET_PARALLELISM),
            conf.getConf(DEFAULT_TARGET_REBALANCE),
            BuildPolicy.ofString(conf.getConf(DEFAULT_TARGET_BUILD_POLICY))
        )
    }
}
case class RelationTarget(
    instanceProperties: Target.Properties,
    relation: Reference[Relation],
    mapping: MappingOutputIdentifier,
    mode: OutputMode = OutputMode.OVERWRITE,
    partition: Map[String,String] = Map.empty,
    parallelism: Int = 16,
    rebalance: Boolean = false,
    buildPolicy: BuildPolicy = BuildPolicy.SMART
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
            phase match {
                case Phase.BUILD|Phase.VERIFY|Phase.TRUNCATE => partition
                case _ => Map()
            }
        )
    }

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = {
        if (mapping.nonEmpty)
            Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY)
        else
            Set(Phase.CREATE, Phase.VERIFY, Phase.DESTROY)
    }

    /**
      * Returns a list of physical resources produced by this target
      * @return
      */
    override def provides(phase: Phase) : Set[ResourceIdentifier] = {
        val rel = relation.value

        phase match {
            case Phase.CREATE => rel.provides(Operation.CREATE)
            case Phase.DESTROY => rel.provides(Operation.DESTROY)
            case Phase.BUILD|Phase.TRUNCATE if mapping.nonEmpty =>
                val partition = this.partition.mapValues(v => SingleValue(v))
                rel.provides(Operation.WRITE, partition)
            case Phase.BUILD =>
                // This special case is mainly for implementing correct dependency management with VIEWs
                rel.provides(Operation.READ)
            case _ => Set()
        }
    }

    /**
      * Returns a list of physical resources required by this target
      * @return
      */
    override def requires(phase: Phase) : Set[ResourceIdentifier] = {
        val rel = relation.value

        phase match {
            case Phase.CREATE => rel.requires(Operation.CREATE)
            case Phase.DESTROY => rel.requires(Operation.DESTROY)
            case Phase.BUILD if mapping.nonEmpty =>
                val partition = this.partition.mapValues(v => SingleValue(v))
                MappingUtils.requires(context, mapping.mapping) ++ rel.requires(Operation.WRITE, partition)
            case Phase.BUILD =>
                // This special case is mainly for implementing correct dependency management with VIEWs
                rel.requires(Operation.READ)
            case Phase.TRUNCATE if mapping.nonEmpty =>
                rel.requires(Operation.WRITE)
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
        val partition = this.partition.mapValues(v => SingleValue(v))
        val rel = relation.value

        phase match {
            case Phase.VALIDATE => No
            case Phase.CREATE =>
                !rel.conforms(execution)
            case Phase.BUILD if mapping.nonEmpty =>
                buildPolicy match {
                    case BuildPolicy.ALWAYS =>
                        Yes
                    case BuildPolicy.IF_EMPTY =>
                        !rel.loaded(execution, partition)
                    case BuildPolicy.SMART =>
                        if (mode == OutputMode.APPEND || partition.isEmpty)
                            Yes
                        else
                            !rel.loaded(execution, partition)
                    case BuildPolicy.COMPAT =>
                        if (mode == OutputMode.APPEND)
                            Yes
                        else
                            !rel.loaded(execution, partition)
                }
            case Phase.BUILD => No
            case Phase.VERIFY => Yes
            case Phase.TRUNCATE =>
                rel.loaded(execution, partition)
            case Phase.DESTROY =>
                rel.exists(execution)
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
            case Phase.BUILD if (mapping.nonEmpty) =>
                val partition = this.partition.mapValues(v => SingleValue(v))
                val mapOut = linker.input(mapping.mapping, mapping.output)
                val relRef = linker.write(relation, partition)
                linker.write(mapOut, relRef)
            case Phase.TRUNCATE =>
                val partition = this.partition.mapValues(v => SingleValue(v))
                linker.write(relation, partition)
            case _ =>
        }
    }

    /**
      * Creates the empty containing (Hive table, SQL table, etc) for holding the data
     *
     * @param executor
      */
    override def create(execution: Execution) : Unit = {
        require(execution != null)

        val rel = relation.value
        if (rel.exists(execution) == Yes) {
            if (rel.conforms(execution) != Yes) {
                logger.info(s"Migrating existing relation '${relation.identifier}'")
                rel.migrate(execution)
            }
        }
        else {
            logger.info(s"Creating relation '${relation.identifier}'")
            rel.create(execution)
        }
    }

    /**
      * Builds the target using the given input tables
      *
      * @param executor
      */
    override def build(executor:Execution) : Unit = {
        require(executor != null)

        if (mapping.nonEmpty) {
            val partition = this.partition.mapValues(v => SingleValue(v))

            logger.info(s"Writing mapping output '${mapping}' to relation '${relation.identifier}' into partition (${partition.map(p => p._1 + "=" + p._2.value).mkString(",")}) with mode '$mode'")
            val map = context.getMapping(mapping.mapping)
            val dfIn = executor.instantiate(map, mapping.output)
            val dfOut =
                if (parallelism <= 0)
                    dfIn
                else if (rebalance)
                    dfIn.repartition(parallelism)
                else
                    dfIn.coalesce(parallelism)

            val dfCount = countRecords(executor, dfOut)
            val rel = relation.value
            rel.write(executor, dfCount, partition, mode)
        }
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param execution
      */
    override def verify2(execution: Execution) : TargetResult = {
        require(execution != null)

        def verifyWithData() : Status = {
            val partition = this.partition.mapValues(v => SingleValue(v))
            val rel = relation.value
            if (rel.loaded(execution, partition) == No) {
                val policy = VerifyPolicy.ofString(execution.flowmanConf.getConf(FlowmanConf.DEFAULT_TARGET_VERIFY_POLICY))
                policy match {
                    case VerifyPolicy.EMPTY_AS_FAILURE =>
                        val error = s"Verification of target '$identifier' failed - partition $partition of relation '${relation.identifier}' does not exist"
                        logger.error(error)
                        throw new VerificationFailedException(identifier, new ExecutionException(error))
                    case VerifyPolicy.EMPTY_AS_SUCCESS | VerifyPolicy.EMPTY_AS_SUCCESS_WITH_ERRORS =>
                        if (rel.exists(execution) != No) {
                            logger.warn(s"Verification of target '$identifier' failed - partition $partition of relation '${relation.identifier}' does not exist. Ignoring.")
                            if (policy == VerifyPolicy.EMPTY_AS_SUCCESS_WITH_ERRORS)
                                Status.SUCCESS_WITH_ERRORS
                            else
                                Status.SUCCESS
                        }
                        else {
                            val error = s"Verification of target '$identifier' failed - relation '${relation.identifier}' does not exist"
                            logger.error(error)
                            throw new VerificationFailedException(identifier, new ExecutionException(error))
                        }
                }
            }
            else {
                Status.SUCCESS
            }
        }
        def verifyWithoutData() : Status = {
            val rel = relation.value
            if (rel.exists(execution) != No) {
                Status.SUCCESS
            }
            else {
                val error = s"Verification of target '$identifier' failed - relation '${relation.identifier}' does not exist"
                logger.error(error)
                throw new VerificationFailedException(identifier, new ExecutionException(error))
            }
        }

        val startTime = Instant.now()
        Try {
            if (mapping.nonEmpty) {
                verifyWithData()
            }
            else {
                verifyWithoutData()
            }
        }
        match {
            case Success(status) => TargetResult(this, Phase.VERIFY, status, startTime)
            case Failure(ex) => TargetResult(this, Phase.VERIFY, ex, startTime)
        }
    }

    /**
      * Cleans the target. This will remove any data in the target for the current partition
      * @param execution
      */
    override def truncate(execution: Execution): Unit = {
        require(execution != null)

        if (mapping.nonEmpty) {
            val partition = this.partition.mapValues(v => SingleValue(v))

            logger.info(s"Truncating partition $partition of relation '${relation.identifier}'")
            val rel = relation.value
            rel.truncate(execution, partition)
        }
    }

    /**
      * Destroys both the logical relation and the physical data
      * @param execution
      */
    override def destroy(execution: Execution) : Unit = {
        require(execution != null)

        logger.info(s"Destroying relation '${relation.identifier}'")
        val rel = relation.value
        rel.destroy(execution)
    }
}



object RelationTargetSpec {
    def apply(name:String, relation:String, partition:Map[String,String]=Map()) : RelationTargetSpec = {
        val spec = new RelationTargetSpec
        spec.name = name
        spec.relation = IdentifierRelationReferenceSpec(relation)
        spec.partition = partition
        spec
    }
}
class RelationTargetSpec extends TargetSpec {
    @JsonProperty(value="mapping", required=true) private var mapping:String = ""
    @JsonProperty(value="relation", required=true) private var relation:RelationReferenceSpec = _
    @JsonProperty(value="mode", required=false) private var mode:Option[String] = None
    @JsonProperty(value="partition", required=false) private var partition:Map[String,String] = Map.empty
    @JsonSchemaInject(json="""{"type": [ "integer", "string" ]}""")
    @JsonProperty(value="parallelism", required=false) private var parallelism:Option[String] = None
    @JsonSchemaInject(json="""{"type": [ "boolean", "string" ]}""")
    @JsonProperty(value="rebalance", required=false) private var rebalance:Option[String] = None
    @JsonProperty(value="buildPolicy", required=false) private var buildPolicy:Option[String] = None

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): RelationTarget = {
        val conf = context.flowmanConf
        RelationTarget(
            instanceProperties(context, properties),
            relation.instantiate(context),
            MappingOutputIdentifier.parse(context.evaluate(mapping)),
            OutputMode.ofString(context.evaluate(mode).getOrElse(conf.getConf(DEFAULT_TARGET_OUTPUT_MODE))),
            context.evaluate(partition),
            context.evaluate(parallelism).map(_.toInt).getOrElse(conf.getConf(DEFAULT_TARGET_PARALLELISM)),
            context.evaluate(rebalance).map(_.toBoolean).getOrElse(conf.getConf(DEFAULT_TARGET_REBALANCE)),
            BuildPolicy.ofString(context.evaluate(buildPolicy).getOrElse(conf.getConf(DEFAULT_TARGET_BUILD_POLICY)))
        )
    }
}
