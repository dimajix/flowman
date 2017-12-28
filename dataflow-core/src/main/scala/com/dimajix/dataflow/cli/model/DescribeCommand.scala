package com.dimajix.dataflow.cli.model

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.cli.Command
import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.Dataflow


class DescribeCommand extends AbstractCommand {
    private val logger = LoggerFactory.getLogger(classOf[DescribeCommand])

    @Argument(usage = "specifies the relation to describe", metaVar = "<relation>", required = true)
    var tablename: String = ""

    override def executeInternal(context:Context, dataflow:Dataflow) : Boolean = {
        logger.info("Describing relation {}", tablename)

        Try {
            val relation = context.getRelation(tablename)
            val table = relation.read(context, null)
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
