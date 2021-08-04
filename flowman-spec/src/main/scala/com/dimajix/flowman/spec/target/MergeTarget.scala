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

package com.dimajix.flowman.spec.target

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.common.Yes
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_RELATION_MIGRATION_POLICY
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_RELATION_MIGRATION_STRATEGY
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_OUTPUT_MODE
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_PARALLELISM
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_REBALANCE
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.metric.LongAccumulatorMetric
import com.dimajix.flowman.metric.Selector
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetInstance
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.sql.functions.count_records


object MergeTarget {
    def apply(context: Context, relation: RelationIdentifier) : MergeTarget = {
        val conf = context.flowmanConf
        new MergeTarget(
            Target.Properties(context),
            MappingOutputIdentifier(""),
            relation,
            conf.getConf(DEFAULT_TARGET_PARALLELISM),
            conf.getConf(DEFAULT_TARGET_REBALANCE)
        )
    }
}

case class MergeTarget(
    instanceProperties: Target.Properties,
    mapping:MappingOutputIdentifier,
    relation: RelationIdentifier,
    parallelism: Int = 16,
    rebalance: Boolean = false
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[MergeTarget])

    /**
      * Returns an instance representing this target with the context
      * @return
      */
    override def instance : TargetInstance = {
        TargetInstance(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            Map()
        )
    }

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
        val rel = context.getRelation(relation)

        phase match {
            case Phase.CREATE|Phase.DESTROY => rel.provides
            case Phase.BUILD => rel.provides ++ rel.resources()
            case _ => Set()
        }
    }

    /**
      * Returns a list of physical resources required by this target
      * @return
      */
    override def requires(phase: Phase) : Set[ResourceIdentifier] = {
        val rel = context.getRelation(relation)

        phase match {
            case Phase.CREATE|Phase.DESTROY => rel.requires
            case Phase.BUILD => rel.requires ++ MappingUtils.requires(context, mapping.mapping)
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
        val rel = context.getRelation(relation)

        phase match {
            case Phase.VALIDATE => No
            case Phase.CREATE =>
                // Since an existing relation might need a migration, we return "unknown"
                if (rel.exists(execution) == Yes)
                    Unknown
                else
                    Yes
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
    override def link(linker: Linker): Unit = {
        linker.input(mapping.mapping, mapping.output)
        linker.write(relation, Map())
    }

    /**
      * Creates the empty containing (Hive table, SQL table, etc) for holding the data
     *
     * @param executor
      */
    override def create(execution: Execution) : Unit = {
        require(execution != null)

        val rel = context.getRelation(relation)
        if (rel.exists(execution) == Yes) {
            logger.info(s"Migrating existing relation '$relation'")
            val migrationPolicy = MigrationPolicy.ofString(execution.flowmanConf.getConf(DEFAULT_RELATION_MIGRATION_POLICY))
            val migrationStrategy = MigrationStrategy.ofString(execution.flowmanConf.getConf(DEFAULT_RELATION_MIGRATION_STRATEGY))
            rel.migrate(execution, migrationPolicy, migrationStrategy)
        }
        else {
            logger.info(s"Creating relation '$relation'")
            rel.create(execution, true)
        }
    }

    /**
      * Builds the target using the given input tables
      *
      * @param executor
      */
    override def build(executor:Execution) : Unit = {
        require(executor != null)

        logger.info(s"Merging mapping '${this.mapping}' into relation '$relation'")
        val mapping = context.getMapping(this.mapping.mapping)
        val dfIn = executor.instantiate(mapping, this.mapping.output)
        val dfOut =
            if (parallelism <= 0)
                dfIn
            else if (rebalance)
                dfIn.repartition(parallelism)
            else
                dfIn.coalesce(parallelism)

        // Setup metric for counting number of records
        val dfCount = countRecords(executor, dfOut)
        val rel = context.getRelation(relation)
        //rel.merge(executor, dfCount)
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    override def verify(executor: Execution) : Unit = {
        require(executor != null)

        val rel = context.getRelation(relation)
        if (rel.loaded(executor) == No) {
            logger.error(s"Verification of target '$identifier' failed - relation '$relation' does not exist")
            throw new VerificationFailedException(identifier)
        }
    }

    /**
      * Cleans the target. This will remove any data in the target for the current partition
      * @param executor
      */
    override def truncate(executor: Execution): Unit = {
        require(executor != null)

        logger.info(s"Truncating relation '$relation'")
        val rel = context.getRelation(relation)
        rel.truncate(executor)
    }

    /**
      * Destroys both the logical relation and the physical data
      * @param executor
      */
    override def destroy(executor: Execution) : Unit = {
        require(executor != null)

        logger.info(s"Destroying relation '$relation'")
        val rel = context.getRelation(relation)
        rel.destroy(executor, true)
    }
}


object MergeTargetSpec {
    def apply(name:String, relation:String) : MergeTargetSpec = {
        val spec = new MergeTargetSpec
        spec.name = name
        spec.relation = relation
        spec
    }
}

class MergeTargetSpec extends TargetSpec {
    @JsonProperty(value="mapping", required=true) private var mapping:String = ""
    @JsonProperty(value="relation", required=true) private var relation:String = _
    @JsonProperty(value="parallelism", required=false) private var parallelism:Option[String] = None
    @JsonProperty(value="rebalance", required=false) private var rebalance:Option[String] = None

    override def instantiate(context: Context): MergeTarget = {
        val conf = context.flowmanConf
        MergeTarget(
            instanceProperties(context),
            MappingOutputIdentifier.parse(context.evaluate(mapping)),
            RelationIdentifier.parse(context.evaluate(relation)),
            context.evaluate(parallelism).map(_.toInt).getOrElse(conf.getConf(DEFAULT_TARGET_PARALLELISM)),
            context.evaluate(rebalance).map(_.toBoolean).getOrElse(conf.getConf(DEFAULT_TARGET_REBALANCE))
        )
    }
}
