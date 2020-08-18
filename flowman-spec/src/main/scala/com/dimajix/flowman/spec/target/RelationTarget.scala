/*
 * Copyright 2018 Kaya Kupferschmidt
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
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
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


object RelationTarget {
    def apply(context: Context, relation: RelationIdentifier) : RelationTarget = {
        new RelationTarget(
            Target.Properties(context),
            MappingOutputIdentifier(""),
            relation,
            OutputMode.OVERWRITE,
            Map(),
            16,
            false
        )
    }
}
case class RelationTarget(
    instanceProperties: Target.Properties,
    mapping:MappingOutputIdentifier,
    relation: RelationIdentifier,
    mode: OutputMode = OutputMode.OVERWRITE,
    partition: Map[String,String] = Map(),
    parallelism: Int = 16,
    rebalance: Boolean = false
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[RelationTarget])

    /**
      * Returns an instance representing this target with the context
      * @return
      */
    override def instance : TargetInstance = {
        TargetInstance(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            partition
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
            Set(Phase.CREATE, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY)
    }

    /**
      * Returns a list of physical resources produced by this target
      * @return
      */
    override def provides(phase: Phase) : Set[ResourceIdentifier] = {
        val partition = this.partition.mapValues(v => SingleValue(v))
        val rel = context.getRelation(relation)

        phase match {
            case Phase.CREATE|Phase.DESTROY => rel.provides
            case Phase.BUILD if mapping.nonEmpty => rel.provides ++ rel.resources(partition)
            case Phase.BUILD => rel.provides
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
            case Phase.BUILD if mapping.nonEmpty => rel.requires ++ MappingUtils.requires(context, mapping.mapping)
            case Phase.BUILD => rel.requires
            case _ => Set()
        }
    }


    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     *
     * @param executor
     * @param phase
     * @return
     */
    override def dirty(executor: Executor, phase: Phase): Trilean = {
        val partition = this.partition.mapValues(v => SingleValue(v))
        val rel = context.getRelation(relation)

        phase match {
            case Phase.CREATE =>
                // Since an existing relation might need a migration, we return "unknown"
                if (rel.exists(executor) == Yes)
                    Unknown
                else
                    Yes
            case Phase.BUILD =>
                if (mode == OutputMode.APPEND) {
                    Yes
                } else {
                    !rel.loaded(executor, partition)
                }
            case Phase.VERIFY => Yes
            case Phase.TRUNCATE =>
                rel.loaded(executor, partition)
            case Phase.DESTROY =>
                rel.exists(executor)
        }
    }

    /**
      * Creates the empty containing (Hive table, SQL table, etc) for holding the data
     *
     * @param executor
      */
    override def create(executor: Executor) : Unit = {
        require(executor != null)

        val rel = context.getRelation(relation)
        if (rel.exists(executor) == Yes) {
            logger.info(s"Migrating existing relation '$relation'")
            rel.migrate(executor)
        }
        else {
            logger.info(s"Creating relation '$relation'")
            rel.create(executor, true)
        }
    }

    /**
      * Builds the target using the given input tables
      *
      * @param executor
      */
    override def build(executor:Executor) : Unit = {
        require(executor != null)

        if (mapping.nonEmpty) {
            val partition = this.partition.mapValues(v => SingleValue(v))

            logger.info(s"Writing mapping '${this.mapping}' to relation '$relation' into partition $partition with mode '$mode'")
            val mapping = context.getMapping(this.mapping.mapping)
            val dfIn = executor.instantiate(mapping, this.mapping.output)
            val dfOut = if (rebalance)
                dfIn.repartition(parallelism)
            else
                dfIn.coalesce(parallelism)

            // Setup metric for counting number of records
            val counter = executor.metrics.findMetric(Selector(Some("target_records"), metadata.asMap))
                .headOption
                .map(_.asInstanceOf[LongAccumulatorMetric].counter)
                .getOrElse {
                    val counter = executor.spark.sparkContext.longAccumulator
                    val metric = LongAccumulatorMetric("target_records", metadata.asMap, counter)
                    executor.metrics.addMetric(metric)
                    counter
                }

            val dfCount = count_records(dfOut, counter)
            val rel = context.getRelation(relation)
            rel.write(executor, dfCount, partition, mode)
        }
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    override def verify(executor: Executor) : Unit = {
        require(executor != null)

        val partition = this.partition.mapValues(v => SingleValue(v))
        val rel = context.getRelation(relation)
        if (rel.loaded(executor, partition) == No) {
            logger.error(s"Verification of target '$identifier' failed - relation '$relation' does not exist")
            throw new VerificationFailedException(identifier)
        }
    }

    /**
      * Cleans the target. This will remove any data in the target for the current partition
      * @param executor
      */
    override def truncate(executor: Executor): Unit = {
        require(executor != null)

        val partition = this.partition.mapValues(v => SingleValue(v))

        logger.info(s"Truncating partition $partition of relation '$relation'")
        val rel = context.getRelation(relation)
        rel.truncate(executor, partition)
    }

    /**
      * Destroys both the logical relation and the physical data
      * @param executor
      */
    override def destroy(executor: Executor) : Unit = {
        require(executor != null)

        logger.info(s"Destroying relation '$relation'")
        val rel = context.getRelation(relation)
        rel.destroy(executor, true)
    }
}



object RelationTargetSpec {
    def apply(name:String, relation:String) : RelationTargetSpec = {
        val spec = new RelationTargetSpec
        spec.name = name
        spec.relation = relation
        spec
    }
}
class RelationTargetSpec extends TargetSpec {
    @JsonProperty(value="mapping", required=true) private var mapping:String = ""
    @JsonProperty(value="relation", required=true) private var relation:String = _
    @JsonProperty(value="mode", required=false) private var mode:String = "overwrite"
    @JsonProperty(value="partition", required=false) private var partition:Map[String,String] = Map()
    @JsonProperty(value="parallelism", required=false) private var parallelism:String = "16"
    @JsonProperty(value="rebalance", required=false) private var rebalance:String = "false"

    override def instantiate(context: Context): RelationTarget = {
        RelationTarget(
            instanceProperties(context),
            MappingOutputIdentifier.parse(context.evaluate(mapping)),
            RelationIdentifier.parse(context.evaluate(relation)),
            OutputMode.ofString(context.evaluate(mode)),
            context.evaluate(partition),
            context.evaluate(parallelism).toInt,
            context.evaluate(rebalance).toBoolean
        )
    }
}
