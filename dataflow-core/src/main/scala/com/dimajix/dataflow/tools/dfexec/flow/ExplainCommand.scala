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


class ExplainCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[DescribeCommand])

    @Option(name="-e", aliases=Array("--extended"), required = false)
    var extended: Boolean = false
    @Argument(usage = "specifies the table to explain", metaVar = "<tablename>", required = true)
    var tablename: String = ""

    override def executeInternal(context:Context, dataflow:Project) : Boolean = {
        logger.info("Explaining table {}", tablename)

        val executor = new Executor(context, dataflow)

        Try {
            val table = executor.instantiate(tablename)
            table.explain(extended)
        } match {
            case Success(_) =>
                logger.info("Successfully finished explaining table")
                true
            case Failure(e) =>
                logger.error("Caught exception while explaining table: {}", e.getMessage)
                logger.error(e.getStackTrace.mkString("\n    at "))
                false
        }
    }
}
