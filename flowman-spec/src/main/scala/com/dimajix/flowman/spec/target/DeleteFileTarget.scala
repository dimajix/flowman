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

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Target


case class DeleteFileTarget(
    instanceProperties:Target.Properties,
    path: Path,
    recursive: Boolean
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[DeleteFileTarget])

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.BUILD, Phase.VERIFY)

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
                val fs = execution.fs
                val file = fs.file(path)
                !file.exists()
            case Phase.VERIFY => Yes
            case _ => No
        }
    }

    /**
     * Build the "count" target by printing the number of records onto the console
     *
     * @param executor
     */
    override def build(executor:Execution) : Unit = {
        val fs = executor.fs
        val file = fs.file(path)
        logger.info(s"Deleting file '$file' (recursive=$recursive)")
        file.delete(recursive)
    }

    /**
     * Performs a verification of the build step or possibly other checks.
     *
     * @param executor
     */
    override def verify(executor: Execution) : Unit = {
        require(executor != null)

        val file = executor.fs.file(path)
        if (file.exists()) {
            logger.error(s"Verification of target '$identifier' failed - location '$path' exists")
            throw new VerificationFailedException(identifier)
        }
    }
}



class DeleteFileTargetSpec extends TargetSpec {
    @JsonProperty(value = "target", required = true) private var target: String = ""
    @JsonProperty(value = "recursive", required = false) private var recursive: String = "true"

    override def instantiate(context: Context): DeleteFileTarget = {
        DeleteFileTarget(
            instanceProperties(context),
            new Path(context.evaluate(target)),
            context.evaluate(recursive).toBoolean
        )
    }
}
