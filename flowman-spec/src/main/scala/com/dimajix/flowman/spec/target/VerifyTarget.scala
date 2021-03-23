/*
 * Copyright 2021 Kaya Kupferschmidt
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

import java.time.Clock

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.ValidationFailedException
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetInstance
import com.dimajix.flowman.spec.assertion.AssertionSpec
import com.dimajix.flowman.util.ConsoleColors.green
import com.dimajix.flowman.util.ConsoleColors.red
import com.dimajix.spark.sql.DataFrameUtils


case class VerifyTarget(
    instanceProperties:Target.Properties,
    assertions:Map[String,Assertion] = Map()
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[VerifyTarget])

    /**
     * Returns an instance representing this target with the context
     *
     * @return
     */
    override def instance: TargetInstance = {
        // Create a custom instance identifier with a timestamp, such that every run is a new instance. Otherwise
        // verification wouldn't be always executed in the presence of a state store.
        TargetInstance(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            Map("verification_ts" -> Clock.systemUTC().millis().toString)
        )
    }

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.VERIFY)

    /**
     * Returns a list of physical resources required by this target
     *
     * @return
     */
    override def requires(phase: Phase): Set[ResourceIdentifier] = {
        phase match {
            case Phase.VERIFY => assertions.flatMap(_._2.requires).toSet
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
            case Phase.VERIFY => Yes
            case _ => No
        }
    }

    /**
     * Performs a verification of the build step or possibly other checks.
     *
     * @param executor
     */
    override protected def verify(execution: Execution): Unit = {
        // Collect all required DataFrames for caching. We assume that each DataFrame might be used in multiple
        // assertions and that the DataFrames aren't very huge (we are talking about tests!)
        val inputDataFrames = assertions
            .flatMap { case(_,instance) =>  instance.inputs }
            .toSeq
            .distinct
            .map(id => execution.instantiate(context.getMapping(id.mapping), id.output))
        val cacheLevel = StorageLevel.NONE // actually disable caching for now

        DataFrameUtils.withCaches(inputDataFrames, cacheLevel) {
            assertions.map { case (name, instance) =>
                val description = instance.description.getOrElse(name)

                if (execution.assert(instance).exists(r => !r.valid)) {
                    logger.error(red(s" ✘ failed: $description"))
                    throw new ValidationFailedException(identifier)
                }
                else {
                    logger.info(green(s" ✓ passed: $description"))
                }
            }
        }
    }
}


class VerifyTargetSpec extends TargetSpec {
    @JsonProperty(value = "assertions", required = true) private var assertions: Map[String,AssertionSpec] = Map()

    override def instantiate(context: Context): VerifyTarget = {
        VerifyTarget(
            instanceProperties(context),
            assertions.map {case(name,assertion) => name -> assertion.instantiate(context) }
        )
    }
}
