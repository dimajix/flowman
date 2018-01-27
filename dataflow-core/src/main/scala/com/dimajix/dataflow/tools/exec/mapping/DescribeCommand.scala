package com.dimajix.dataflow.tools.exec.mapping

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.spec.TableIdentifier
import com.dimajix.dataflow.tools.exec.ActionCommand


class DescribeCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[DescribeCommand])

    @Argument(usage = "specifies the table to describe", metaVar = "<tablename>", required = true)
    var tablename: String = ""


    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        logger.info("Describing table {}", tablename)

        Try {
            val table = executor.instantiate(TableIdentifier.parse(tablename))
            table.describe()
        } match {
            case Success(_) =>
                logger.info("Successfully finished describing table")
                true
            case Failure(e) =>
                logger.error("Caught exception while describing table: {}", e.getMessage)
                logger.error(e.getStackTrace.mkString("\n    at "))
                false
        }
    }
}
