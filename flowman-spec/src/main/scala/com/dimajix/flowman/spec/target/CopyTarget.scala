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
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.dataset.DatasetSpec
import com.dimajix.flowman.types.SchemaWriter
import com.dimajix.flowman.types.StructType


object CopyTarget {
    case class Schema(
        file:Path,
        format:String
    )
}
final case class CopyTarget(
    instanceProperties:Target.Properties,
    source:Dataset,
    target:Dataset,
    schema:Option[CopyTarget.Schema] = None,
    mode:OutputMode = OutputMode.OVERWRITE,
    parallelism:Int = 16,
    rebalance: Boolean = false
) extends BaseTarget {
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
            case Phase.BUILD => target.provides ++ schema.map(s => ResourceIdentifier.ofFile(s.file)).toSet
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
            case Phase.BUILD => source.requires
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
            case Phase.BUILD => !target.exists(execution)
            case Phase.VERIFY => Yes
            case Phase.TRUNCATE|Phase.DESTROY => target.exists(execution)
            case _ => No
        }
    }

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    override protected def build(execution: Execution): Unit = {
        require(execution != null)

        logger.info(s"Copying dataset ${source.name} to ${target.name}")

        val dfIn = source.read(execution)
        val dfOut =
            if (parallelism <= 0)
                dfIn
            else if (rebalance)
                dfIn.repartition(parallelism)
            else
                dfIn.coalesce(parallelism)

        val dfCount = countRecords(execution, dfOut)
        target.write(execution, dfCount, mode)

        schema.foreach { spec =>
            logger.info(s"Writing schema to file '${spec.file}'")
            val schema = source.describe(execution).getOrElse(StructType.of(dfCount.schema))
            val file = context.fs.file(spec.file)
            new SchemaWriter(schema.fields).format(spec.format).save(file)
        }
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param execution
      */
    override protected def verify(execution: Execution): Unit = {
        require(execution != null)

        if (target.exists(execution) == No) {
            val error = s"Verification of target '$identifier' failed - target '${target.name}' does not exist"
            logger.error(error)
            throw new VerificationFailedException(identifier, new ExecutionException(error))
        }

        schema.foreach { spec =>
            val file = execution.fs.file(spec.file)
            if (!file.exists()) {
                val error = s"Verification of target '$identifier' failed - schema file '${spec.file}' does not exist"
                logger.error(error)
                throw new VerificationFailedException(identifier, new ExecutionException(error))
            }
        }
    }

    /**
      * Deletes data of a specific target
      *
      * @param execution
      */
    override protected def truncate(execution: Execution): Unit = {
        require(execution != null)

        target.clean(execution)

        schema.foreach { spec =>
            val outputFile = execution.fs.file(spec.file)
            if (outputFile.exists()) {
                logger.info(s"Removing schema file '${spec.file}'")
                outputFile.delete()
            }
        }
    }

    /**
      * Completely destroys the resource associated with this target. This will delete both the phyiscal data and
      * the table definition
      *
      * @param execution
      */
    override def destroy(execution: Execution): Unit = {
        schema.foreach { spec =>
            val outputFile = execution.fs.file(spec.file)
            if (outputFile.exists()) {
                logger.info(s"Removing schema file '${spec.file}'")
                outputFile.delete()
            }
        }
    }
}


object CopyTargetSpec {
    class Schema {
        @JsonProperty(value="file", required=true) private var file:String = _
        @JsonProperty(value="format", required=false) private var format:String = "avro"

        def instantiate(context: Context): CopyTarget.Schema = {
            CopyTarget.Schema(
                new Path(context.evaluate(file)),
                context.evaluate(format)
            )
        }

    }
}
class CopyTargetSpec extends TargetSpec {
    @JsonProperty(value = "source", required = true) private var source: DatasetSpec = _
    @JsonProperty(value = "target", required = true) private var target: DatasetSpec = _
    @JsonProperty(value = "schema", required = false) private var schema: Option[CopyTargetSpec.Schema] = None
    @JsonProperty(value = "mode", required = false) private var mode: Option[String] = None
    @JsonProperty(value = "parallelism", required = false) private var parallelism: Option[String] = None
    @JsonProperty(value = "rebalance", required=false) private var rebalance:Option[String] = None

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): CopyTarget = {
        val conf = context.flowmanConf
        CopyTarget(
            instanceProperties(context, properties),
            source.instantiate(context),
            target.instantiate(context),
            schema.map(_.instantiate(context)),
            OutputMode.ofString(context.evaluate(mode).getOrElse(conf.getConf(DEFAULT_TARGET_OUTPUT_MODE))),
            context.evaluate(parallelism).map(_.toInt).getOrElse(conf.getConf(DEFAULT_TARGET_PARALLELISM)),
            context.evaluate(rebalance).map(_.toBoolean).getOrElse(conf.getConf(DEFAULT_TARGET_REBALANCE))
        )
    }
}
