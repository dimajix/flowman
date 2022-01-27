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
import java.time.Instant

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.AssertionRunner
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.ErrorMode
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.execution.ValidationFailedException
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.spec.assertion.AssertionSpec


case class VerifyTarget(
    instanceProperties:Target.Properties,
    assertions:Map[String,Assertion] = Map(),
    errorMode:ErrorMode = ErrorMode.FAIL_AT_END
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[VerifyTarget])

    /**
     * Returns an instance representing this target with the context
     *
     * @return
     */
    override def digest(phase:Phase): TargetDigest = {
        // Create a custom instance identifier with a timestamp, such that every run is a new instance. Otherwise
        // verification wouldn't be always executed in the presence of a state store.
        TargetDigest(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            phase,
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
    protected override def verify2(execution: Execution): TargetResult = {
        val startTime = Instant.now()
        val runner = new AssertionRunner(context, execution, cacheLevel = StorageLevel.NONE)
        val result = runner.run(assertions.values.toList, keepGoing = errorMode != ErrorMode.FAIL_FAST)

        val status = Status.ofAll(result.map(_.status))
        if (!status.success) {
            if (errorMode != ErrorMode.FAIL_NEVER || result.exists(_.numExceptions > 0)) {
                logger.error(s"Verification '$identifier' failed.")
                TargetResult(this, Phase.VERIFY, result, new VerificationFailedException(identifier, result.flatMap(_.exception).headOption.orNull), startTime)
            }
            else {
                logger.error(s"Verification '$identifier' failed - the result is marked as 'success with errors'.")
                TargetResult(this, Phase.VERIFY, result, Status.SUCCESS_WITH_ERRORS, startTime)
            }
        }
        else {
            TargetResult(this, Phase.VERIFY, result, status, startTime)
        }
    }
}


class VerifyTargetSpec extends TargetSpec {
    @JsonDeserialize(converter=classOf[AssertionSpec.NameResolver])
    @JsonProperty(value = "assertions", required = true) private var assertions: Map[String,AssertionSpec] = Map()
    @JsonProperty(value = "mode", required = false) private var mode: String = "failAtEnd"

    override def instantiate(context: Context): VerifyTarget = {
        VerifyTarget(
            instanceProperties(context),
            assertions.map {case(name,assertion) => name -> assertion.instantiate(context) },
            ErrorMode.ofString(context.evaluate(mode))
        )
    }
}
