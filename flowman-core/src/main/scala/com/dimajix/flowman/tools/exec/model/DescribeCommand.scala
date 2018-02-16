package com.dimajix.flowman.tools.exec.model

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.tools.exec.ActionCommand


class DescribeCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[DescribeCommand])

    @Argument(usage = "specifies the relation to describe", metaVar = "<relation>", required = true)
    var tablename: String = ""

    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        logger.info("Describing relation {}", tablename)
        val context = executor.context

        Try {
            val relation = context.getRelation(RelationIdentifier.parse(tablename))
            val table = relation.read(executor, null)
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
