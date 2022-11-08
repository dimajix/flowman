/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.ExecutionException
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.types.SchemaWriter


case class SchemaTarget(
    instanceProperties: Target.Properties,
    schema: Schema,
    file: Path,
    format: String
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[SchemaTarget])

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
    override def provides(phase:Phase) : Set[ResourceIdentifier] = {
        phase match {
            case Phase.BUILD => Set(ResourceIdentifier.ofFile(file))
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
            case Phase.BUILD =>
                val dst = execution.fs.file(file)
                !dst.exists()
            case Phase.VERIFY => Yes
            case Phase.TRUNCATE|Phase.DESTROY =>
                val dst = execution.fs.file(file)
                dst.exists()
            case _ => No
        }
    }

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    override def build(executor: Execution): Unit = {
        require(executor != null)

        logger.info(s"Writing schema to file '$file'")
        val outputFile = context.fs.file(file)
        new SchemaWriter(schema.fields).format(format).save(outputFile)
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    override def verify(executor: Execution): Unit = {
        require(executor != null)

        val outputFile = executor.fs.file(file)
        if (!outputFile.exists()) {
            val error = s"Verification of target '$identifier' failed - schema file '$file' does not exist"
            logger.error(error)
            throw new VerificationFailedException(identifier, new ExecutionException(error))
        }
    }

    /**
      * Deletes data of a specific target
      *
      * @param executor
      */
    override def truncate(executor: Execution): Unit = {
        require(executor != null)

        val outputFile = executor.fs.file(file)
        if (outputFile.exists()) {
            logger.info(s"Removing schema file '$file'")
            outputFile.delete()
        }
    }

    /**
      * Completely destroys the resource associated with this target. This will delete both the phyiscal data and
      * the table definition
      *
      * @param executor
      */
    override def destroy(executor: Execution): Unit = {
        truncate(executor)
    }
}



class SchemaTargetSpec extends TargetSpec {
    @JsonProperty(value="schema", required=true) private var schema:SchemaSpec = _
    @JsonProperty(value="file", required=true) private var file:String = _
    @JsonProperty(value="format", required=false) private var format:String = "avro"

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): SchemaTarget = {
        SchemaTarget(
            instanceProperties(context, properties),
            schema.instantiate(context),
            new Path(context.evaluate(file)),
            context.evaluate(format)
        )
    }
}
