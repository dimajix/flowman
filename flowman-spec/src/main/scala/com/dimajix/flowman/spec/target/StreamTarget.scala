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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target


case class StreamTarget(
    instanceProperties:Target.Properties,
    mapping:MappingOutputIdentifier,
    relation:RelationIdentifier,
    mode:OutputMode,
    parallelism:Int,
    checkpointLocation:Path
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[StreamTarget])

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
        phase match {
            case Phase.BUILD => MappingUtils.requires(context, mapping.mapping)
            case _ => Set()
        }
    }

    /**
      * Creates the empty containing (Hive tabl, SQL table, etc) for holding the data
      * @param executor
      */
    override def create(executor: Executor) : Unit = {
        require(executor != null)

        val rel = context.getRelation(relation)
        if (rel.exists(executor)) {
            logger.info(s"Migrating existing relation '$relation'")
            rel.migrate(executor)
        }
        else {
            logger.info(s"Creating relation '$relation'")
            rel.create(executor, true)
        }
    }

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    override def build(executor: Executor): Unit = {
        logger.info(s"Writing mapping '${this.mapping}' to streaming relation '$relation' using mode '$mode' and checkpoint location '$checkpointLocation'")
        val mapping = context.getMapping(this.mapping.mapping)
        val rel = context.getRelation(relation)
        val table = executor.instantiate(mapping, this.mapping.output).coalesce(parallelism)
        rel.writeStream(executor, table, mode, checkpointLocation)
    }

    /**
      * Clean up streaming target. Actually this method delegates the work to the relation target
      *
      * @param executor
      */
    override def truncate(executor: Executor): Unit = {
        logger.info(s"Cleaining streaming relation '$relation'")
        val rel = context.getRelation(relation)
        rel.truncate(executor)
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




class StreamTargetSpec extends TargetSpec {
    @JsonProperty(value="input", required=true) private var input:String = _
    @JsonProperty(value="relation", required=true) private var relation:String = _
    @JsonProperty(value="mode", required=false) private var mode:String = OutputMode.UPDATE.toString
    @JsonProperty(value="checkpointLocation", required=false) private var checkpointLocation:String = _
    @JsonProperty(value="parallelism", required=false) private var parallelism:String = "16"

    override def instantiate(context: Context): Target = {
        val  mode = OutputMode.ofString(context.evaluate(this.mode))
        val checkpointLocation = Option(context.evaluate(this.checkpointLocation))
            .map(_.trim)
            .filter(_.nonEmpty)
            .getOrElse("/tmp/flowman-streaming-sink-" + name + "-" + System.currentTimeMillis())


        StreamTarget(
            instanceProperties(context),
            MappingOutputIdentifier.parse(context.evaluate(input)),
            RelationIdentifier.parse(context.evaluate(relation)),
            mode,
            context.evaluate(parallelism).toInt,
            new Path(checkpointLocation)
        )
    }
}
