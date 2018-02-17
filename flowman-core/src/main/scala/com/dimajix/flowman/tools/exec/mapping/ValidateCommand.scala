package com.dimajix.flowman.tools.exec.mapping

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.TableIdentifier
import com.dimajix.flowman.tools.exec.ActionCommand


class ValidateCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[ValidateCommand])

    @Argument(usage = "specifies mappings to validate", metaVar = "<mapping>")
    var mappings: Array[String] = Array()

    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        implicit val context = executor.context
        logger.info("Validating mappings {}", if (mappings != null) mappings.mkString(",") else "all")

        // Then execute output operations
        Try {
            val mappingNames =
                if (mappings.nonEmpty)
                    mappings.toSeq
                else
                    project.mappings.keys.toSeq

            val tables = mappingNames.map(TableIdentifier.parse)
            tables.forall(table => executor.instantiate(table) != null)
        } match {
            case Success(true) =>
                logger.info("Successfully validated mappings")
                true
            case Success(false) =>
                logger.error("Validation of mappings failed")
                false
            case Failure(e) =>
                logger.error("Caught exception while validating mapping", e)
                false
        }
    }
}
