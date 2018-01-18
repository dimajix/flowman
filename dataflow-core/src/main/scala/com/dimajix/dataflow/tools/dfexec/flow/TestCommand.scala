package com.dimajix.dataflow.tools.dfexec.flow

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.tools.dfexec.ActionCommand


class TestCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[TestCommand])

    @Argument(usage = "specifies testcases to execute", metaVar = "<testcases>")
    var testcases: Array[String] = Array()
    @Option(name = "--all", usage = "runs all test cases, even the disabled ones")
    var all: Boolean = false

    override def executeInternal(context:Context, project:Project) : Boolean = {
        logger.info("Processing tests {}", if (testcases != null) testcases.mkString(",") else "all")

        implicit val icontext = context

        val executor = new Executor(context, project)

        // First execute tests
        val result = Try {
            val toRun =
                if (all)
                    project.testcases.keys.toSeq
                else if (testcases.nonEmpty)
                    testcases.toSeq
                else
                    project.testcases.filter(_._2.enabled).keys.toSeq
            executor.test(toRun)
        } match {
            case Success(true) =>
                logger.info("Successfully finished execution of all testcases in DataFlow")
                true
            case Success(false) =>
                logger.error("Execution of testcases in DataFlow failed")
                false
            case Failure(e) =>
                logger.error("Caught exception while executing testcases in dataflow: {}", e.getMessage)
                logger.error(e.getStackTrace.mkString("\n    at "))
                false
        }

        // Cleanup caches, but after printing error message. Otherwise it looks confusing when the error occured
        executor.cleanup()

        result
    }

}
