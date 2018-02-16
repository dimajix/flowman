package com.dimajix.flowman.tools.exec.model

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.tools.exec.ActionCommand


class ShowCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[ShowCommand])

    @Option(name="-n", aliases=Array("--limit"), usage="Specifies maximimum number of rows to print", metaVar="<limit>", required = false)
    var limit: Int = 10
    @Argument(usage = "specifies the relation to show", metaVar = "<relation>", required = true)
    var tablename: String = ""


    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        logger.info("Showing first {} rows of relation {}", limit:Any, tablename:Any)
        val context = executor.context

        Try {
            val relation = context.getRelation(RelationIdentifier.parse(tablename))
            val table = relation.read(executor, null)
            table.limit(limit).show(truncate = false)
        } match {
            case Success(_) =>
                logger.info("Successfully finished dumping relation")
                true
            case Failure(e) =>
                logger.error("Caught exception while dumping relation: {}", e.getMessage)
                logger.error(e.getStackTrace.mkString("\n    at "))
                false
        }
    }
}
