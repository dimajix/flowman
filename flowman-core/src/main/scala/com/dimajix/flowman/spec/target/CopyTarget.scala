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
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.spec.dataset.Dataset
import com.dimajix.flowman.spec.dataset.DatasetSpec
import com.dimajix.flowman.transforms.SchemaEnforcer
import com.dimajix.flowman.types.SchemaWriter


object CopyTarget {
    case class Schema(
        file:Path,
        format:String
    )
}
case class CopyTarget(
    instanceProperties:Target.Properties,
    source:Dataset,
    target:Dataset,
    schema:Option[CopyTarget.Schema],
    parallelism:Int,
    mode:String
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[CopyTarget])

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
            case Phase.BUILD => target.resources ++ schema.map(s => ResourceIdentifier.ofFile(s.file)).toSet
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
            case Phase.BUILD => source.resources
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
        require(executor != null)

        logger.info(s"Copying dataset ${source.name} to ${target.name}")

        val data = source.read(executor, None).coalesce(parallelism)
        val conformed = target.schema.map { schema =>
            val xfs = SchemaEnforcer(schema.sparkType)
            xfs.transform(data)
        }.getOrElse(data)
        target.write(executor, conformed, mode)

        schema.foreach { spec =>
            logger.info(s"Writing schema to file '${spec.file}'")
            val schema = source.schema.getOrElse(throw new IllegalArgumentException("Schema not available"))
            val file = context.fs.file(spec.file)
            new SchemaWriter(schema.fields).format(spec.format).save(file)
        }
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    override protected def verify(executor: Executor): Unit = {
        require(executor != null)

        if (!target.exists(executor)) {
            throw new VerificationFailedException(identifier)
        }

        schema.foreach { spec =>
            val file = executor.fs.file(spec.file)
            if (!file.exists()) {
                logger.error(s"Verification of target '$identifier' failed - schema file '${spec.file}' does not exist")
                throw new VerificationFailedException(identifier)
            }
        }
    }

    /**
      * Deletes data of a specific target
      *
      * @param executor
      */
    override protected def truncate(executor: Executor): Unit = {
        require(executor != null)

        target.clean(executor)

        schema.foreach { spec =>
            val outputFile = executor.fs.file(spec.file)
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
      * @param executor
      */
    override def destroy(executor: Executor): Unit = {
        schema.foreach { spec =>
            val outputFile = executor.fs.file(spec.file)
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
    @JsonProperty(value = "parallelism", required = false) private var parallelism: String = "16"
    @JsonProperty(value = "mode", required = false) private var mode: String = "overwrite"

    override def instantiate(context: Context): CopyTarget = {
        CopyTarget(
            instanceProperties(context),
            source.instantiate(context),
            target.instantiate(context),
            schema.map(_.instantiate(context)),
            context.evaluate(parallelism).toInt,
            context.evaluate(mode)
        )
    }
}
