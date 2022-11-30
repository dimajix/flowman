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

import java.io.File
import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.common.Yes
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_RELATION_MIGRATION_POLICY
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_RELATION_MIGRATION_STRATEGY
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_PARALLELISM
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_REBALANCE
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.StreamingActivity
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.relation.RelationReferenceSpec
import com.dimajix.flowman.types.SingleValue


case class StreamTarget(
    instanceProperties:Target.Properties,
    mapping:MappingOutputIdentifier,
    relation:Reference[Relation],
    mode:OutputMode,
    trigger:Trigger,
    parallelism:Int,
    rebalance: Boolean = false,
    checkpointLocation:Path
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[StreamTarget])

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = {
        Set(Phase.CREATE, Phase.BUILD, Phase.TRUNCATE, Phase.DESTROY)
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
            case Phase.BUILD => MappingUtils.requires(context, mapping.mapping) ++ rel.requires(Operation.WRITE)
            case Phase.TRUNCATE => rel.requires(Operation.WRITE)
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
                // Since an existing relation might need a migration, we return "unknown"
                if (rel.exists(execution) == Yes)
                    Unknown
                else
                    Yes
            case Phase.BUILD => Yes
            case Phase.VERIFY => No
            case Phase.TRUNCATE =>
                rel.loaded(execution, Map())
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
            case Phase.BUILD =>
                val mapOut = linker.input(mapping.mapping, mapping.output)
                val relRef = linker.write(relation, Map.empty[String,SingleValue])
                linker.write(mapOut, relRef)
            case Phase.TRUNCATE|Phase.DESTROY =>
                linker.write(relation, Map.empty[String,SingleValue])
            case _ =>
        }
    }

    /**
      * Creates the empty containing (Hive tabl, SQL table, etc) for holding the data
      * @param execution
      */
    override def create(execution: Execution) : Unit = {
        require(execution != null)

        val rel = relation.value
        if (rel.exists(execution) == Yes) {
            logger.info(s"Migrating existing relation '${relation.identifier}'")
            val migrationPolicy = MigrationPolicy.ofString(execution.flowmanConf.getConf(DEFAULT_RELATION_MIGRATION_POLICY))
            val migrationStrategy = MigrationStrategy.ofString(execution.flowmanConf.getConf(DEFAULT_RELATION_MIGRATION_STRATEGY))
            rel.migrate(execution, migrationPolicy, migrationStrategy)
        }
        else {
            logger.info(s"Creating relation '${relation.identifier}'")
            rel.create(execution)
        }
    }

    /**
      * Abstract method which will perform the output activity. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param execution
      */
    override def build(execution: Execution): Unit = {
        require(execution != null)

        logger.info(s"Streaming mapping '${this.mapping}' to relation '${relation.identifier}' with trigger '$trigger', mode '$mode' and checkpoint '$checkpointLocation'")
        val mapping = context.getMapping(this.mapping.mapping)
        val rel = relation.value
        val dfIn = execution.instantiate(mapping, this.mapping.output)
        val dfOut =
            if (parallelism <= 0)
                dfIn
            else if (rebalance)
                dfIn.repartition(parallelism)
            else
                dfIn.coalesce(parallelism)

        val query = rel.writeStream(execution, dfOut, mode, trigger, checkpointLocation)
        query.exception.foreach(ex => throw new RuntimeException("Error while starting streaming query", ex))

        // When the trigger is 'one', wait until the query has finished
        if (trigger == Trigger.Once()) {
            query.awaitTermination()
        }
        else {
            // Otherwise create background activity
            execution.activities.post(StreamingActivity(identifier.toString, query))
        }
    }

    /**
      * Clean up streaming target. Actually this method delegates the work to the relation target
      *
      * @param executor
      */
    override def truncate(execution: Execution): Unit = {
        require(execution != null)

        logger.info(s"Truncating relation '${relation.identifier}'")
        val rel = relation.value
        rel.truncate(execution)
    }

    /**
      * Destroys both the logical relation and the physical data
      * @param executor
      */
    override def destroy(execution: Execution) : Unit = {
        require(execution != null)

        logger.info(s"Destroying relation '${relation.identifier}'")
        val rel = relation.value
        rel.destroy(execution)
    }
}




class StreamTargetSpec extends TargetSpec {
    @JsonProperty(value="mapping", required=true) private var mapping:String = _
    @JsonProperty(value="relation", required=true) private var relation:RelationReferenceSpec = _
    @JsonProperty(value="mode", required=false) private var mode:String = OutputMode.UPDATE.toString
    @JsonProperty(value="trigger", required=false) private var trigger:Option[String] = None
    @JsonProperty(value="checkpointLocation", required=false) private var checkpointLocation:Option[String] = None
    @JsonProperty(value="parallelism", required=false) private var parallelism:Option[String] = None
    @JsonProperty(value="rebalance", required=false) private var rebalance:Option[String] = None

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): Target = {
        val conf = context.flowmanConf
        val checkpointLocation = context.evaluate(this.checkpointLocation)
            .getOrElse(new File(System.getProperty("java.io.tmpdir"), "flowman-streaming-sink-" + name + "-" + System.currentTimeMillis()).toString)

        val trigger = context.evaluate(this.trigger).map(_.toLowerCase(Locale.ROOT)) match {
            case Some("once") => Trigger.Once()
            case Some(interval) => Trigger.ProcessingTime(interval)
            case None => Trigger.ProcessingTime(0L)
        }

        StreamTarget(
            instanceProperties(context, properties),
            MappingOutputIdentifier.parse(context.evaluate(mapping)),
            relation.instantiate(context),
            OutputMode.ofString(context.evaluate(this.mode)),
            trigger,
            context.evaluate(parallelism).map(_.toInt).getOrElse(conf.getConf(DEFAULT_TARGET_PARALLELISM)),
            context.evaluate(rebalance).map(_.toBoolean).getOrElse(conf.getConf(DEFAULT_TARGET_REBALANCE)),
            new Path(checkpointLocation)
        )
    }
}
