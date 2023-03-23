/*
 * Copyright (C) 2021 The Flowman Authors
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
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.common.Yes
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_PARALLELISM
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_REBALANCE
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.DeleteClause
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.ExecutionException
import com.dimajix.flowman.execution.InsertClause
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.MergeClause
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.UpdateClause
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
import com.dimajix.flowman.spec.relation.IdentifierRelationReferenceSpec
import com.dimajix.flowman.spec.relation.RelationReferenceSpec
import com.dimajix.flowman.spec.target.MergeTargetSpec.MergeClauseSpec
import com.dimajix.flowman.types.SingleValue


object MergeTarget {
    def apply(context: Context, relation: RelationIdentifier, mapping: MappingOutputIdentifier, mergeKey:Seq[String], clauses:Seq[MergeClause]) : MergeTarget = {
        val conf = context.flowmanConf
        new MergeTarget(
            Target.Properties(context, relation.name, "merge"),
            RelationReference(context, relation),
            mapping,
            mergeKey,
            None,
            clauses,
            conf.getConf(DEFAULT_TARGET_PARALLELISM),
            conf.getConf(DEFAULT_TARGET_REBALANCE)
        )
    }
}

final case class MergeTarget(
    instanceProperties: Target.Properties,
    relation: Reference[Relation],
    mapping: MappingOutputIdentifier,
    key: Seq[String],
    condition: Option[String],
    clauses: Seq[MergeClause],
    parallelism: Int = 16,
    rebalance: Boolean = false
) extends BaseTarget {
    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = {
        Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY)
    }

    /**
      * Returns a list of physical resources produced by this target
      * @return
      */
    override def provides(phase: Phase) : Set[ResourceIdentifier] = {
        val rel = relation.value

        phase match {
            case Phase.CREATE|Phase.DESTROY => rel.provides(Operation.CREATE)
            case Phase.BUILD|Phase.TRUNCATE => rel.provides(Operation.WRITE)
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
            case Phase.CREATE|Phase.DESTROY => rel.requires(Operation.CREATE)
            case Phase.BUILD|Phase.TRUNCATE => MappingUtils.requires(context, mapping.mapping) ++ rel.requires(Operation.WRITE)
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
            case Phase.VALIDATE => No
            case Phase.CREATE =>
                !rel.conforms(execution)
            case Phase.BUILD =>
                Unknown
            case Phase.VERIFY => Yes
            case Phase.TRUNCATE =>
                rel.loaded(execution)
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
            case Phase.BUILD =>
                linker.input(mapping.mapping, mapping.output)
                linker.write(relation, Map.empty[String,SingleValue])
                // TODO: Link columns
            case Phase.TRUNCATE =>
                linker.write(relation, Map.empty[String,SingleValue])
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
      * @param execution
      */
    override def build(execution:Execution) : Unit = {
        require(execution != null)

        logger.info(s"Merging mapping '${this.mapping}' into relation '${relation.identifier}'")
        val mapping = context.getMapping(this.mapping.mapping)
        val dfIn = execution.instantiate(mapping, this.mapping.output)
        val dfOut =
            if (parallelism <= 0)
                dfIn
            else if (rebalance)
                dfIn.repartition(parallelism)
            else
                dfIn.coalesce(parallelism)

        // Setup metric for counting number of records
        val dfCount = countRecords(execution, dfOut)

        // Create merge condition
        val conds = key.map(k => col("target." + k) === col("source." + k)) ++ this.condition.map(expr)
        val condition =
            if (conds.nonEmpty)
                Some(conds.reduce(_ && _))
            else
                None
        val rel = relation.value
        rel.merge(execution, dfCount, condition, clauses)
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param execution
      */
    override def verify(execution: Execution) : Unit = {
        require(execution != null)

        val rel = relation.value
        if (rel.loaded(execution) == No) {
            val error = s"Verification of target '$identifier' failed - relation '${relation.identifier}' does not exist"
            logger.error(error)
            throw new VerificationFailedException(identifier, new ExecutionException(error))
        }
    }

    /**
      * Cleans the target. This will remove any data in the target for the current partition
      * @param execution
      */
    override def truncate(execution: Execution): Unit = {
        require(execution != null)

        logger.info(s"Truncating relation '${relation.identifier}'")
        val rel = relation.value
        rel.truncate(execution)
    }

    /**
      * Destroys both the logical relation and the physical data
      * @param execution
      */
    override def destroy(execution: Execution) : Unit = {
        require(execution != null)

        logger.info(s"Destroying relation '${relation.identifier}''")
        val rel = relation.value
        rel.destroy(execution)
    }
}


object MergeTargetSpec {
    def apply(name:String, relation:String) : MergeTargetSpec = {
        val spec = new MergeTargetSpec
        spec.name = name
        spec.relation = IdentifierRelationReferenceSpec(relation)
        spec
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "action")
    @JsonSubTypes(value = Array(
        new JsonSubTypes.Type(name = "insert", value = classOf[InsertClauseSpec]),
        new JsonSubTypes.Type(name = "update", value = classOf[UpdateClauseSpec]),
        new JsonSubTypes.Type(name = "delete", value = classOf[DeleteClauseSpec])
    ))
    abstract sealed class MergeClauseSpec {
        def instantiate(context: Context) : MergeClause
    }
    final class InsertClauseSpec extends MergeClauseSpec {
        @JsonProperty(value="condition", required=false) private var condition:Option[String] = None
        @JsonProperty(value="columns", required = true) private var columns: Map[String,String] = Map()

        def instantiate(context: Context) : MergeClause = {
            InsertClause(
                context.evaluate(condition).map(expr),
                context.evaluate(columns).map(kv => kv._1 -> expr(kv._2))
            )
        }
    }
    final class UpdateClauseSpec extends MergeClauseSpec {
        @JsonProperty(value="condition", required=false) private var condition:Option[String] = None
        @JsonProperty(value="columns", required = true) private var columns: Map[String,String] = Map()

        def instantiate(context: Context) : MergeClause = {
            UpdateClause(
                context.evaluate(condition).map(expr),
                context.evaluate(columns).map(kv => kv._1 -> expr(kv._2))
            )
        }
    }
    final class DeleteClauseSpec extends MergeClauseSpec {
        @JsonProperty(value="condition", required=false) private var condition:Option[String] = None

        def instantiate(context: Context) : MergeClause = {
            DeleteClause(
                context.evaluate(condition).map(expr)
            )
        }
    }
}
class MergeTargetSpec extends TargetSpec {
    @JsonProperty(value="mapping", required=true) private var mapping:String = ""
    @JsonProperty(value="relation", required=true) private var relation:RelationReferenceSpec = _
    @JsonProperty(value="parallelism", required=false) private var parallelism:Option[String] = None
    @JsonProperty(value="rebalance", required=false) private var rebalance:Option[String] = None
    @JsonProperty(value="mergeKey", required=false) private var mergeKey:Seq[String] = Seq()
    @JsonProperty(value="condition", required=false) private var mergeCondition:Option[String] = None
    @JsonProperty(value="clauses", required=false) private var clauses:Seq[MergeClauseSpec] = Seq()

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): MergeTarget = {
        val conf = context.flowmanConf
        MergeTarget(
            instanceProperties(context, properties),
            relation.instantiate(context),
            MappingOutputIdentifier.parse(context.evaluate(mapping)),
            mergeKey.map(context.evaluate),
            mergeCondition.map(context.evaluate),
            clauses.map(_.instantiate(context)),
            context.evaluate(parallelism).map(_.toInt).getOrElse(conf.getConf(DEFAULT_TARGET_PARALLELISM)),
            context.evaluate(rebalance).map(_.toBoolean).getOrElse(conf.getConf(DEFAULT_TARGET_REBALANCE))
        )
    }
}
