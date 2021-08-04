/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.StreamingOperation
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.types.SingleValue


case class StreamTarget(
    instanceProperties:Target.Properties,
    mapping:MappingOutputIdentifier,
    relation:RelationIdentifier,
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
        val rel = context.getRelation(relation)

        phase match {
            case Phase.CREATE|Phase.DESTROY => rel.provides
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
            case Phase.BUILD => MappingUtils.requires(context, mapping.mapping)
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
    override def link(linker: Linker): Unit = {
        linker.input(mapping.mapping, mapping.output)
        linker.write(relation, Map())
    }

    /**
      * Creates the empty containing (Hive tabl, SQL table, etc) for holding the data
      * @param execution
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
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param execution
      */
    override def build(execution: Execution): Unit = {
        require(execution != null)

        logger.info(s"Streaming mapping '${this.mapping}' to relation '$relation' with trigger '$trigger', mode '$mode' and checkpoint '$checkpointLocation'")
        val mapping = context.getMapping(this.mapping.mapping)
        val rel = context.getRelation(relation)
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
            // Otherwise create background operation
            execution.operations.post(StreamingOperation(identifier.toString, query))
        }
    }

    /**
      * Clean up streaming target. Actually this method delegates the work to the relation target
      *
      * @param executor
      */
    override def truncate(execution: Execution): Unit = {
        require(execution != null)

        logger.info(s"Truncating relation '$relation'")
        val rel = context.getRelation(relation)
        rel.truncate(execution)
    }

    /**
      * Destroys both the logical relation and the physical data
      * @param executor
      */
    override def destroy(execution: Execution) : Unit = {
        require(execution != null)

        logger.info(s"Destroying relation '$relation'")
        val rel = context.getRelation(relation)
        rel.destroy(execution, true)
    }
}




class StreamTargetSpec extends TargetSpec {
    @JsonProperty(value="mapping", required=true) private var mapping:String = _
    @JsonProperty(value="relation", required=true) private var relation:String = _
    @JsonProperty(value="mode", required=false) private var mode:String = OutputMode.UPDATE.toString
    @JsonProperty(value="trigger", required=false) private var trigger:Option[String] = None
    @JsonProperty(value="checkpointLocation", required=false) private var checkpointLocation:Option[String] = None
    @JsonProperty(value="parallelism", required=false) private var parallelism:Option[String] = None
    @JsonProperty(value="rebalance", required=false) private var rebalance:Option[String] = None

    override def instantiate(context: Context): Target = {
        val conf = context.flowmanConf
        val checkpointLocation = context.evaluate(this.checkpointLocation)
            .getOrElse(new File(System.getProperty("java.io.tmpdir"), "flowman-streaming-sink-" + name + "-" + System.currentTimeMillis()).toString)

        val trigger = context.evaluate(this.trigger).map(_.toLowerCase(Locale.ROOT)) match {
            case Some("once") => Trigger.Once()
            case Some(interval) => Trigger.ProcessingTime(interval)
            case None => Trigger.ProcessingTime(0L)
        }

        StreamTarget(
            instanceProperties(context),
            MappingOutputIdentifier.parse(context.evaluate(mapping)),
            RelationIdentifier.parse(context.evaluate(relation)),
            OutputMode.ofString(context.evaluate(this.mode)),
            trigger,
            context.evaluate(parallelism).map(_.toInt).getOrElse(conf.getConf(DEFAULT_TARGET_PARALLELISM)),
            context.evaluate(rebalance).map(_.toBoolean).getOrElse(conf.getConf(DEFAULT_TARGET_REBALANCE)),
            new Path(checkpointLocation)
        )
    }
}
