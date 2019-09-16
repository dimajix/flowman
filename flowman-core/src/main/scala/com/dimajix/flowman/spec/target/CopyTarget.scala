/*
 * Copyright 2019 Kaya Kupferschmidt
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
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.spec.dataset.Dataset
import com.dimajix.flowman.spec.dataset.DatasetSpec
import com.dimajix.flowman.transforms.SchemaEnforcer


case class CopyTarget(
    instanceProperties:Target.Properties,
    source:Dataset,
    target:Dataset,
    parallelism:Int,
    mode:String
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[CopyTarget])


    /**
      * Creates the resource associated with this target. This may be a Hive table or a JDBC table. This method
      * will not provide the data itself, it will only create the container
      *
      * @param executor
      */
    override protected def create(executor: Executor): Unit = {}

    override protected def migrate(executor: Executor): Unit = {}

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    override protected def build(executor: Executor): Unit = {
        logger.info(s"Copying dataset ${source.name} to ${target.name}")

        val data = source.read(executor, None).coalesce(parallelism)
        val conformed = target.schema.map { schema =>
            val xfs = SchemaEnforcer(schema.sparkType)
            xfs.transform(data)
        }.getOrElse(data)
        target.write(executor, conformed, mode)
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    override protected def verify(executor: Executor): Unit = {}

    /**
      * Deletes data of a specific target
      *
      * @param executor
      */
    override protected def truncate(executor: Executor): Unit = {}

    /**
      * Completely destroys the resource associated with this target. This will delete both the phyiscal data and
      * the table definition
      *
      * @param executor
      */
    override protected def destroy(executor: Executor): Unit = {}

    /**
      * Returns a list of physical resources produced by this target
      *
      * @return
      */
    override def provides(phase: Phase): Seq[ResourceIdentifier] = {
        phase match {
            case Phase.BUILD => target.resources
            case _ => Seq()
        }
    }

    /**
      * Returns a list of physical resources required by this target
      *
      * @return
      */
    override def requires(phase: Phase): Seq[ResourceIdentifier] = {
        phase match {
            case Phase.BUILD => source.resources
            case _ => Seq()
        }
    }
}


class CopyTargetSpec extends TargetSpec {
    @JsonProperty(value = "source", required = true) private var source: DatasetSpec = _
    @JsonProperty(value = "target", required = true) private var target: DatasetSpec = _
    @JsonProperty(value = "parallelism", required = false) private var parallelism: String = "16"
    @JsonProperty(value = "mode", required = false) private var mode: String = "overwrite"

    override def instantiate(context: Context): CopyTarget = {
        CopyTarget(
            instanceProperties(context),
            source.instantiate(context),
            target.instantiate(context),
            context.evaluate(parallelism).toInt,
            context.evaluate(mode)
        )
    }
}
