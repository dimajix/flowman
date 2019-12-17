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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.types.SingleValue


object RelationTarget {
    def apply(context: Context, relation: RelationIdentifier) = {
        new RelationTarget(
            Target.Properties(context),
            MappingOutputIdentifier(""),
            relation,
            "overwrite",
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
    mode: String,
    partition: Map[String,String],
    parallelism: Int,
    rebalance: Boolean
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[RelationTarget])

    /**
      * Returns an instance representing this target with the context
      * @return
      */
    override def instance : TargetInstance = {
        TargetInstance(
            Option(namespace).map(_.name).getOrElse(""),
            Option(project).map(_.name).getOrElse(""),
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
            Set(Phase.CREATE, Phase.MIGRATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY)
        else
            Set(Phase.CREATE, Phase.MIGRATE, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY)
    }

    /**
      * Returns a list of physical resources produced by this target
      * @return
      */
    override def provides(phase: Phase) : Set[ResourceIdentifier] = {
        val partition = this.partition.mapValues(v => SingleValue(v))
        val rel = context.getRelation(relation)

        phase match {
            case Phase.CREATE|Phase.MIGRATE|Phase.DESTROY => rel.provides
            case Phase.BUILD if (mapping.nonEmpty) => rel.provides ++ rel.resources(partition)
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
            case Phase.CREATE|Phase.MIGRATE|Phase.DESTROY => rel.requires
            case Phase.BUILD if (mapping.nonEmpty) => rel.requires ++ MappingUtils.requires(context, mapping.mapping)
            case _ => Set()
        }
    }

    /**
      * Creates the empty containing (Hive tabl, SQL table, etc) for holding the data
      * @param executor
      */
    override def create(executor: Executor) : Unit = {
        require(executor != null)

        logger.info(s"Creating relation '$relation'")
        val rel = context.getRelation(relation)
        rel.create(executor, true)
    }

    /**
      * Tries to migrate the given target to the newest schema
      * @param executor
      */
    override def migrate(executor: Executor) : Unit = {
        require(executor != null)

        logger.info(s"Migrating relation '$relation'")
        val rel = context.getRelation(relation)
        rel.migrate(executor)
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

            logger.info(s"Writing mapping '${this.mapping}' to relation '$relation' into partition $partition")
            val mapping = context.getMapping(this.mapping.mapping)
            val dfIn = executor.instantiate(mapping, this.mapping.output)
            val table = if (rebalance)
                dfIn.repartition(parallelism)
            else
                dfIn.coalesce(parallelism)

            val rel = context.getRelation(relation)
            rel.write(executor, table, partition, mode)
        }
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    override def verify(executor: Executor) : Unit = {
        require(executor != null)

        val rel = context.getRelation(relation)
        if (!rel.exists(executor)) {
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
    @JsonProperty(value="input", required=true) private var input:String = ""
    @JsonProperty(value="relation", required=true) private var relation:String = _
    @JsonProperty(value="mode", required=false) private var mode:String = "overwrite"
    @JsonProperty(value="partition", required=false) private var partition:Map[String,String] = Map()
    @JsonProperty(value="parallelism", required=false) private var parallelism:String = "16"
    @JsonProperty(value="rebalance", required=false) private var rebalance:String = "false"

    override def instantiate(context: Context): RelationTarget = {
        RelationTarget(
            instanceProperties(context),
            MappingOutputIdentifier.parse(context.evaluate(input)),
            RelationIdentifier.parse(context.evaluate(relation)),
            context.evaluate(mode),
            context.evaluate(partition),
            context.evaluate(parallelism).toInt,
            context.evaluate(rebalance).toBoolean
        )
    }
}
