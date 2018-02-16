package com.dimajix.flowman.tools.exec.output

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.tools.exec.ActionCommand


class ValidateCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[ValidateCommand])

    @Argument(usage = "specifies outputs to process", metaVar = "<output>")
    var outputs: Array[String] = Array()
    @Option(name = "-a", aliases=Array("--all"), usage = "runs all outputs, even the disabled ones")
    var all: Boolean = false

    def executeInternal(executor:Executor, project: Project) : Boolean = {
        implicit val context = executor.context
        logger.info("Validating outputs {}", if (outputs != null) outputs.mkString(",") else "all")

        // Then execute output operations
        Try {
            val outputNames =
                if (all)
                    project.outputs.keys.toSeq
                else if (outputs.nonEmpty)
                    outputs.toSeq
                else
                    project.outputs.filter(_._2.enabled).keys.toSeq

            val tables = outputNames.map(project.outputs).flatMap(_.dependencies)
            tables.forall(table => executor.instantiate(table) != null)
        } match {
            case Success(true) =>
                logger.info("Successfully validated outputs")
                true
            case Success(false) =>
                logger.error("Validation of outputs failed")
                false
            case Failure(e) =>
                logger.error("Caught exception while validating output", e)
                // logger.error(e.getStackTrace.mkString("\n    at "))
                false
        }
    }
}
