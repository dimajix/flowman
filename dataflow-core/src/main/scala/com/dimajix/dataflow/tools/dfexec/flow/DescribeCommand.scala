package com.dimajix.dataflow.tools.dfexec.flow

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.tools.dfexec.ActionCommand


class DescribeCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[DescribeCommand])

    @Argument(usage = "specifies the table to describe", metaVar = "<tablename>", required = true)
    var tablename: String = ""

    override def executeInternal(context:Context, dataflow:Project) : Boolean = {
        logger.info("Describing table {}", tablename)

        val executor = new Executor(context, dataflow)

        Try {
            val table = executor.instantiate(tablename)
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
