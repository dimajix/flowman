/*
 * Copyright (C) 2019 The Flowman Authors
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

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_OUTPUT_MODE
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_PARALLELISM
import com.dimajix.flowman.config.FlowmanConf.DEFAULT_TARGET_REBALANCE
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.ExecutionException
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.fs.HadoopUtils
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest


object FileTarget {
    def apply(context: Context, mapping:MappingOutputIdentifier, location:Path, format:String, options:Map[String,String]) = {
        val conf = context.flowmanConf
        new FileTarget(
            Target.Properties(context),
            mapping,
            location,
            format,
            options,
            OutputMode.ofString(conf.getConf(DEFAULT_TARGET_OUTPUT_MODE)),
            conf.getConf(DEFAULT_TARGET_PARALLELISM),
            conf.getConf(DEFAULT_TARGET_REBALANCE)
        )
    }
}
final case class FileTarget(
    instanceProperties: Target.Properties,
    mapping:MappingOutputIdentifier,
    location:Path,
    format:String,
    options:Map[String,String],
    mode:OutputMode,
    parallelism: Int,
    rebalance: Boolean = false
) extends BaseTarget {
    private val qualifiedLocation = {
        val conf = context.hadoopConf
        val fs = location.getFileSystem(conf)
        location.makeQualified(fs.getUri, fs.getWorkingDirectory)
    }

    /**
      * Returns an instance representing this target with the context
      *
      * @return
      */
    override def digest(phase:Phase): TargetDigest = {
        TargetDigest(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            phase,
            Map("location" -> qualifiedLocation.toString)
        )
    }

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY)

    /**
      * Returns a list of physical resources produced by this target
      *
      * @return
      */
    override def provides(phase: Phase): Set[ResourceIdentifier] = Set(
        ResourceIdentifier.ofFile(qualifiedLocation)
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

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     *
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase): Trilean = {
        phase match {
            case Phase.CREATE =>
                val fs = qualifiedLocation.getFileSystem(execution.spark.sparkContext.hadoopConfiguration)
                !fs.getFileStatus(qualifiedLocation).isDirectory
            case Phase.BUILD =>
                val fs = qualifiedLocation.getFileSystem(execution.spark.sparkContext.hadoopConfiguration)
                !HadoopUtils.isValidFileData(fs, qualifiedLocation)
            case Phase.VERIFY => Yes
            case Phase.TRUNCATE =>
                val fs = qualifiedLocation.getFileSystem(execution.spark.sparkContext.hadoopConfiguration)
                fs.listStatus(qualifiedLocation).nonEmpty
            case Phase.DESTROY =>
                val fs = qualifiedLocation.getFileSystem(execution.spark.sparkContext.hadoopConfiguration)
                fs.exists(qualifiedLocation)
            case _ => No
        }
    }

    override def create(execution: Execution) : Unit = {
        require(execution != null)

        val fs = qualifiedLocation.getFileSystem(execution.spark.sparkContext.hadoopConfiguration)
        if (!fs.getFileStatus(qualifiedLocation).isDirectory) {
            logger.info(s"Creating directory '$qualifiedLocation' for file relation '$identifier'")
            fs.mkdirs(qualifiedLocation)
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

        logger.info(s"Writing mapping output '${mapping}' to directory '$qualifiedLocation'")
        val map = context.getMapping(mapping.mapping)
        val dfIn = execution.instantiate(map, mapping.output)
        val table = {
            if (parallelism <= 0)
                dfIn
            else if (rebalance)
                dfIn.repartition(parallelism)
            else
                dfIn.coalesce(parallelism)
        }

        val dfCount = countRecords(execution, table)
        dfCount.write
            .options(options)
            .format(format)
            .mode(mode.batchMode)
            .save(qualifiedLocation.toString)
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param execution
      */
    override def verify(execution: Execution) : Unit = {
        require(execution != null)

        val file = execution.fs.file(qualifiedLocation)
        if (!file.exists()) {
            val error = s"Verification of target '$identifier' failed - location '$qualifiedLocation' does not exist"
            logger.error(error)
            throw new VerificationFailedException(identifier, new ExecutionException(error))
        }
    }

    /**
      * Cleans up a specific target
      *
      * @param execution
      */
    override def truncate(execution: Execution): Unit = {
        require(execution != null)

        val fs = qualifiedLocation.getFileSystem(execution.spark.sparkContext.hadoopConfiguration)
        if (fs.getFileStatus(qualifiedLocation).isDirectory) {
            logger.info(s"Truncating directory '$qualifiedLocation' of file relation '$identifier'")
            val files = fs.listStatus(qualifiedLocation)
            files.foreach(file => fs.delete(file.getPath, true))
        }
    }

    override def destroy(execution: Execution) : Unit = {
        require(execution != null)

        val fs = qualifiedLocation.getFileSystem(execution.spark.sparkContext.hadoopConfiguration)
        if (fs.exists(qualifiedLocation)) {
            logger.info(s"Deleting directory '$qualifiedLocation' of file relation '$identifier'")
            fs.delete(qualifiedLocation, true)
        }
    }
}



class FileTargetSpec extends TargetSpec {
    @JsonProperty(value="mapping", required=true) private var mapping:String = _
    @JsonProperty(value="location", required=true) private var location:String = _
    @JsonProperty(value="format", required=false) private var format:String = "csv"
    @JsonProperty(value="mode", required=false) private var mode:Option[String] = None
    @JsonProperty(value="options", required=false) private var options:Map[String,String] = Map()
    @JsonProperty(value="parallelism", required=false) private var parallelism:Option[String] = None
    @JsonProperty(value="rebalance", required=false) private var rebalance:Option[String] = None

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): FileTarget = {
        val conf = context.flowmanConf
        FileTarget(
            instanceProperties(context, properties),
            MappingOutputIdentifier.parse(context.evaluate(mapping)),
            new Path(context.evaluate(location)),
            context.evaluate(format),
            context.evaluate(options),
            OutputMode.ofString(context.evaluate(mode).getOrElse(conf.getConf(DEFAULT_TARGET_OUTPUT_MODE))),
            context.evaluate(parallelism).map(_.toInt).getOrElse(conf.getConf(DEFAULT_TARGET_PARALLELISM)),
            context.evaluate(rebalance).map(_.toBoolean).getOrElse(conf.getConf(DEFAULT_TARGET_REBALANCE))
        )
    }
}
