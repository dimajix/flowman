package com.dimajix.flowman.execution

import scala.util.control.NonFatal

import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.AssertionSuiteResult
import com.dimajix.flowman.util.ConsoleColors.green
import com.dimajix.flowman.util.ConsoleColors.red
import com.dimajix.spark.sql.DataFrameUtils


class AssertionRunner(
    context:Context,
    execution:Execution,
    cacheLevel:StorageLevel=StorageLevel.MEMORY_AND_DISK
) {
    private val logger = LoggerFactory.getLogger(classOf[AssertionRunner])

    def run(assertions:Seq[Assertion], dryRun:Boolean=false, keepGoing:Boolean=false) : Seq[AssertionSuiteResult] = {
        // Collect all required DataFrames for caching. We assume that each DataFrame might be used in multiple
        // assertions and that the DataFrames aren't very huge (we are talking about tests!)
        val inputDataFrames = assertions
            .flatMap(instance => if(!dryRun) instance.inputs else Seq())
            .distinct
            .map(id => execution.instantiate(context.getMapping(id.mapping), id.output))

        DataFrameUtils.withCaches(inputDataFrames, cacheLevel) {
            assertions.map { instance =>
                val name = instance.name
                val description = instance.description.getOrElse(name)
                val status = if (!dryRun) {
                    try {
                        execution.assert(instance)
                    }
                    catch {
                        case NonFatal(ex) if keepGoing =>
                            logger.error(s" ✘ exception: $description: ${ex.getMessage}")
                            Seq(AssertionResult(name, false, exception = true))
                    }
                }
                else {
                    Seq()
                }

                val success = status.forall(r => r.valid)
                if (!success) {
                    logger.error(red(s" ✘ failed: $description"))
                }
                else {
                    logger.info(green(s" ✓ passed: $description"))
                }

                AssertionSuiteResult(name, description, status)
            }
        }
    }
}
