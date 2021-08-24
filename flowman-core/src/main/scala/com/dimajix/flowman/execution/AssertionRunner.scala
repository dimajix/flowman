package com.dimajix.flowman.execution

import scala.util.control.NonFatal

import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionTestResult
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.util.ConsoleColors.green
import com.dimajix.flowman.util.ConsoleColors.red
import com.dimajix.spark.sql.DataFrameUtils


class AssertionRunner(
    context:Context,
    execution:Execution,
    cacheLevel:StorageLevel=StorageLevel.MEMORY_AND_DISK
) {
    private val logger = LoggerFactory.getLogger(classOf[AssertionRunner])

    def run(assertions:Seq[Assertion], dryRun:Boolean=false, keepGoing:Boolean=false) : Seq[AssertionResult] = {
        // Collect all required DataFrames for caching. We assume that each DataFrame might be used in multiple
        // assertions and that the DataFrames aren't very huge (we are talking about tests!)
        val inputDataFrames = assertions
            .flatMap(instance => if(!dryRun) instance.inputs else Seq())
            .distinct
            .map(id => execution.instantiate(context.getMapping(id.mapping), id.output))

        DataFrameUtils.withCaches(inputDataFrames, cacheLevel) {
            var error = false
            assertions.map { instance =>
                if (!dryRun && (!error || keepGoing)) {
                    val status = try {
                        execution.assert(instance)
                    }
                    catch {
                        case NonFatal(ex) =>
                            val name = instance.name
                            val description = instance.description
                            logger.error(s" ✘ exception: ${description.getOrElse(name)}: ${ex.getMessage}")
                            if (!keepGoing)
                                throw ex

                            AssertionResult(
                                name,
                                description,
                                Seq(AssertionTestResult(name, false, exception = true))
                            )
                    }

                    val success = status.success
                    error |= !success

                    val name = status.name
                    val description = status.description.getOrElse(name)
                    if (!success) {
                        logger.error(red(s" ✘ failed: $description"))
                    }
                    else {
                        logger.info(green(s" ✓ passed: $description"))
                    }

                    status
                }
                else {
                    val name = instance.name
                    val description = instance.description
                    logger.info(green(s" ✓ skipped: ${description.getOrElse(name)}"))
                    AssertionResult(name, description, Seq())
                }
            }
        }
    }
}
