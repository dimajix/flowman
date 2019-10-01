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

import java.nio.charset.Charset

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.spec
import com.dimajix.flowman.spec.ResourceIdentifier


case class MergeFilesTarget(
    instanceProperties:Target.Properties,
    source:Path,
    target:Path,
    delimiter:String = null,
    overwrite:Boolean = true
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[MergeFilesTarget])

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY)

    /**
     * Returns a list of physical resources produced by this target
     *
     * @return
     */
    override def provides(phase: Phase): Set[ResourceIdentifier] = {
        phase match {
            case Phase.BUILD => Set(ResourceIdentifier.ofLocal(target))
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
            case Phase.BUILD => Set(ResourceIdentifier.ofFile(source))
            case _ => Set()
        }
    }

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    override protected def build(executor: Executor): Unit = {
        val fs = executor.fs
        val src = fs.file(source)
        val dst = fs.local(target)
        val delimiter = Option(this.delimiter).map(_.getBytes(Charset.forName("UTF-8"))).filter(_.nonEmpty)

        logger.info(s"Merging remote files in '$src' to local file '$dst' (overwrite=$overwrite)")
        val output = dst.create(overwrite)
        try {
            fs.file(source)
                .list()
                .filter(_.isFile())
                .sortBy(_.toString)
                .foreach(file => {
                    val input = file.open()
                    try {
                        IOUtils.copyBytes(input, output, 16384, false)
                        delimiter.foreach(output.write)
                    }
                    finally {
                        input.close()
                    }
                })
        }
        finally {
            output.close()
        }
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
            logger.error(s"Verification of target '$identifier' failed - local file '$target' does not exist")
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

        val outputFile = executor.fs.local(target)
        if (outputFile.exists()) {
            logger.info(s"Removing local file '$target'")
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
        truncate(executor)
    }
}




class MergeFilesTargetSpec extends TargetSpec {
    @JsonProperty(value = "source", required = true) private var source: String = ""
    @JsonProperty(value = "target", required = true) private var target: String = ""
    @JsonProperty(value = "delimiter", required = true) private var delimiter: String = _
    @JsonProperty(value = "overwrite", required = false) private var overwrite: String = "true"

    override def instantiate(context: Context): MergeFilesTarget = {
        spec.target.MergeFilesTarget(
            instanceProperties(context),
            new Path(context.evaluate(source)),
            new Path(context.evaluate(target)),
            context.evaluate(delimiter),
            context.evaluate(overwrite).toBoolean
        )
    }
}
