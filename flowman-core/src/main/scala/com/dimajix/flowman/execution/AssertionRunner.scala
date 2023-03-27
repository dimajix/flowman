/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.flowman.execution

import java.time.Instant

import scala.util.control.NonFatal

import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.common.text.ConsoleColors.green
import com.dimajix.common.text.ConsoleColors.red
import com.dimajix.common.text.ConsoleColors.yellow
import com.dimajix.spark.sql.DataFrameUtils


class AssertionRunner(
    context:Context,
    execution:Execution,
    cacheLevel:StorageLevel=StorageLevel.MEMORY_AND_DISK
) {
    private val logger = execution.loggerFactory.getLogger(classOf[AssertionRunner].getName)

    def run(assertions:Seq[Assertion], dryRun:Boolean=false, keepGoing:Boolean=false) : Seq[AssertionResult] = {
        // Collect all required DataFrames for caching. We assume that each DataFrame might be used in multiple
        // assertions and that the DataFrames aren't very huge (we are talking about tests!)
        val inputDataFrames = assertions
            .flatMap(instance => if(!dryRun) instance.inputs else Seq())
            .distinct
            .map(id => execution.instantiate(context.getMapping(id.mapping), id.output))

        DataFrameUtils.withCaches(inputDataFrames, cacheLevel) {
            var error = false
            assertions.map { assertion =>
                val startTime = Instant.now()
                execution.monitorAssertion(assertion) { execution =>
                    if (!error || keepGoing) {
                        val result =
                            if (!dryRun) {
                                execution.assert(assertion)
                            }
                            else {
                                AssertionResult(assertion, Instant.now())
                            }
                        error |= !result.success

                        logResult(result)

                        result
                    }
                    else {
                        val description = assertion.description.getOrElse(assertion.name)
                        logger.info(yellow(s" - skipped: $description}"))
                        AssertionResult(assertion, startTime)
                    }
                }
            }
        }
    }

    private def logResult(result:AssertionResult) : Unit = {
        val description = result.description.getOrElse(result.name)
        result.exception match {
            case Some(ex) =>
                logger.error(red(s" ✘ exception $description: ${ex.getMessage}"))
            case None if (!result.success) =>
                logger.error(red(s" ✘ failed: $description"))
                // If an error occured, walk through the children to find a possible exception or failure to display
                result.children.filter(_.failure).foreach { result =>
                    result.exception match {
                        case Some(ex) =>
                            logger.error(red(s"   ✘ exception ${result.name}: ${ex.getMessage}"))
                        case None =>
                            logger.error(red(s"   ✘ failed ${result.name}"))
                    }
                }
            case None =>
                logger.info(green(s" ✓ passed: $description"))
        }
    }
}
