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
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.ResourceIdentifier


object FileTarget {
    def apply(context: Context, mapping:MappingOutputIdentifier, location:Path, format:String, options:Map[String,String]) = {
        new FileTarget(
            Target.Properties(context),
            mapping,
            location,
            format,
            options,
            "overwrite",
            16,
            false
        )
    }
}
case class FileTarget(
    instanceProperties: Target.Properties,
    mapping:MappingOutputIdentifier,
    location:Path,
    format:String,
    options:Map[String,String],
    mode: String,
    parallelism: Int,
    rebalance: Boolean
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[FileTarget])

    /**
      * Returns an instance representing this target with the context
      *
      * @return
      */
    override def instance: TargetInstance = {
        TargetInstance(
            Option(namespace).map(_.name).getOrElse(""),
            Option(project).map(_.name).getOrElse(""),
            name,
            Map("location" -> location.toString)
        )
    }

    /**
      * Returns a list of physical resources produced by this target
      *
      * @return
      */
    override def provides(phase: Phase): Set[ResourceIdentifier] = Set(
        ResourceIdentifier.ofFile(location)
    )

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

    override def create(executor: Executor) : Unit = {
        require(executor != null)

        val fs = location.getFileSystem(executor.spark.sparkContext.hadoopConfiguration)
        if (!fs.isDirectory(location)) {
            logger.info(s"Creating directory '$location' for file relation '$identifier'")
            fs.mkdirs(location)
        }
    }

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    override def build(executor: Executor): Unit = {
        require(executor != null)

        val mapping = context.getMapping(this.mapping.mapping)
        val dfIn = executor.instantiate(mapping, this.mapping.output)
        val table = if (rebalance)
            dfIn.repartition(parallelism)
        else
            dfIn.coalesce(parallelism)

        logger.info(s"Writing mapping '$mapping' to directory '$location'")
        table.write
            .options(options)
            .format(format)
            .mode(mode)
            .save(location.toString)
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    override def verify(executor: Executor) : Unit = {
        require(executor != null)

        val file = executor.fs.file(location)
        if (!file.exists()) {
            logger.error(s"Verification of target '$identifier' failed - location '$location' does not exist")
            throw new VerificationFailedException(identifier)
        }
    }

    /**
      * Cleans up a specific target
      *
      * @param executor
      */
    override def truncate(executor: Executor): Unit = {
        require(executor != null)

        val fs = location.getFileSystem(executor.spark.sparkContext.hadoopConfiguration)
        if (fs.isDirectory(location)) {
            logger.info(s"Truncating directory '$location' of file relation '$identifier'")
            val files = fs.listStatus(location)
            files.foreach(file => fs.delete(file.getPath, true))
        }
    }

    override def destroy(executor: Executor) : Unit = {
        require(executor != null)

        val fs = location.getFileSystem(executor.spark.sparkContext.hadoopConfiguration)
        if (fs.exists(location)) {
            logger.info(s"Deleting directory '$location' of file relation '$identifier'")
            fs.delete(location, true)
        }
    }
}



class FileTargetSpec extends TargetSpec {
    @JsonProperty(value = "input", required=true) private var input:String = _
    @JsonProperty(value="location", required=true) private var location:String = _
    @JsonProperty(value="format", required=false) private var format:String = "csv"
    @JsonProperty(value="mode", required=false) private var mode:String = "overwrite"
    @JsonProperty(value="options", required=false) private var options:Map[String,String] = Map()
    @JsonProperty(value="parallelism", required=false) private var parallelism:String = "16"
    @JsonProperty(value="rebalance", required=false) private var rebalance:String = "false"

    override def instantiate(context: Context): FileTarget = {
        FileTarget(
            instanceProperties(context),
            MappingOutputIdentifier.parse(context.evaluate(input)),
            new Path(context.evaluate(location)),
            context.evaluate(format),
            context.evaluate(options),
            context.evaluate(mode),
            context.evaluate(parallelism).toInt,
            context.evaluate(rebalance).toBoolean
        )
    }
}
