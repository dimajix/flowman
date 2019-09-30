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
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.spec.ResourceIdentifier


case class PutFileTarget(
    instanceProperties:Target.Properties,
    source:Path,
    target:Path,
    overwrite:Boolean
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[PutFileTarget])

    /**
      * Creates the resource associated with this target. This may be a Hive table or a JDBC table. This method
      * will not provide the data itself, it will only create the container
      *
      * @param executor
      */
    override protected def create(executor: Executor): Unit = {}

    override protected def migrate(executor: Executor): Unit = {}

    override protected def build(executor:Executor) : Unit = {
        val fs = executor.fs
        val src = fs.local(source)
        val dst = fs.file(target)
        logger.info(s"Putting local file '$src' to remote destination '$dst' (overwrite=$overwrite)")
        src.copy(dst, overwrite)
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    override protected def verify(executor: Executor): Unit = {
        require(executor != null)

        val file = executor.fs.local(target)
        if (!file.exists()) {
            logger.error(s"Verification of target '$identifier' failed - file '$target' does not exist")
            throw new VerificationFailedException(identifier)
        }
    }

    /**
      * Deletes data of a specific target
      *
      * @param executor
      */
    override protected def truncate(executor: Executor): Unit = {
        require(executor != null)

        val outputFile = executor.fs.file(target)
        if (outputFile.exists()) {
            logger.info(s"Removing file '$target'")
            outputFile.delete()
        }
    }

    /**
      * Completely destroys the resource associated with this target. This will delete both the phyiscal data and
      * the table definition
      *
      * @param executor
      */
    override protected def destroy(executor: Executor): Unit = {
        require(executor != null)

        val outputFile = executor.fs.file(target)
        if (outputFile.exists()) {
            logger.info(s"Removing file '$target'")
            outputFile.delete()
        }
    }

    /**
      * Returns a list of physical resources produced by this target
      *
      * @return
      */
    override def provides(phase: Phase): Set[ResourceIdentifier] = {
        phase match {
            case Phase.BUILD => Set(ResourceIdentifier.ofFile(target))
            case _ => Set()
        }
    }

    /**
      * Returns a list of physical resources required by this target
      *
      * @return
      */
    override def requires(phase: Phase): Set[ResourceIdentifier] = {
        phase match {
            case Phase.BUILD => Set(ResourceIdentifier.ofLocal(source))
            case _ => Set()
        }
    }
}



class PutFileTargetSpec extends TargetSpec {
    @JsonProperty(value = "source", required = true) private var source: String = ""
    @JsonProperty(value = "tarPut", required = true) private var tarPut: String = ""
    @JsonProperty(value = "overwrite", required = false) private var overwrite: String = "true"

    override def instantiate(context: Context): PutFileTarget = {
        PutFileTarget(
            instanceProperties(context),
            new Path(context.evaluate(source)),
            new Path(context.evaluate(tarPut)),
            context.evaluate(overwrite).toBoolean
        )
    }
}
