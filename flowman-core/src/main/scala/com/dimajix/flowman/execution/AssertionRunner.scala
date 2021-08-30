package com.dimajix.flowman.execution

import scala.util.control.NonFatal

import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.JobRunnerImpl.RunnerJobToken
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionTestResult
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetInstance
import com.dimajix.flowman.util.ConsoleColors.green
import com.dimajix.flowman.util.ConsoleColors.red
import com.dimajix.flowman.util.ConsoleColors.yellow
import com.dimajix.flowman.util.withShutdownHook
import com.dimajix.spark.sql.DataFrameUtils


class AssertionRunner(
    context:Context,
    execution:Execution,
    cacheLevel:StorageLevel=StorageLevel.MEMORY_AND_DISK,
    hooks:Seq[(RunnerListener,Token)] = Seq()
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
                withListeners(instance) {
                    if (!error || keepGoing) {
                        val result = executeAssertion(instance, dryRun)
                        val success = result.success
                        error |= !success

                        val name = result.name
                        val description = result.description.getOrElse(name)
                        if (!success) {
                            logger.error(red(s" ✘ failed: $description"))
                        }
                        else {
                            logger.info(green(s" ✓ passed: $description"))
                        }

                        result
                    }
                    else {
                        val name = instance.name
                        val description = instance.description.getOrElse(name)
                        logger.info(yellow(s" - skipped: $description}"))
                        AssertionResult(instance, Seq())
                    }
                }
            }
        }
    }

    private def executeAssertion(assertion: Assertion, dryRun:Boolean) : AssertionResult = {
        try {
            if (!dryRun) {
                execution.assert(assertion)
            }
            else {
                AssertionResult(assertion, Seq())
            }
        }
        catch {
            case NonFatal(ex) =>
                val name = assertion.name
                val description = assertion.description.getOrElse(name)
                logger.error(s" ✘ exception: $description: ${ex.getMessage}")
                AssertionResult(assertion, ex)
        }
    }

    private def withListeners(assertion: Assertion)(fn: => AssertionResult) : AssertionResult = {
        def startAssertion() : Seq[(RunnerListener, AssertionToken)] = {
            hooks.flatMap { case(listener,jobToken) =>
                try {
                    Some((listener, listener.startAssertion(assertion, Some(jobToken))))
                }
                catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on startAssertion: ${ex.toString}.")
                        None
                }
            }
        }

        def finishAssertion(tokens:Seq[(RunnerListener, AssertionToken)], status:AssertionResult) : Unit = {
            tokens.foreach { case(listener, token) =>
                try {
                    listener.finishAssertion(token, status)
                } catch {
                    case NonFatal(ex) =>
                        logger.warn(s"Execution listener threw exception on finishAssertion: ${ex.toString}.")
                }
            }
        }

        val tokens = startAssertion()
        val status = fn
        finishAssertion(tokens, status)
        status
    }
}
