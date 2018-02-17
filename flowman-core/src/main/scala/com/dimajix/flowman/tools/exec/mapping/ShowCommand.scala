package com.dimajix.flowman.tools.exec.mapping

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.TableIdentifier
import com.dimajix.flowman.tools.exec.ActionCommand


class ShowCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[ShowCommand])

    @Option(name="-n", aliases=Array("--limit"), usage="Specifies maximimum number of rows to print", metaVar="<limit>", required = false)
    var limit: Int = 10
    @Argument(usage = "specifies the table to show", metaVar = "<tablename>", required = true)
    var tablename: String = ""


    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        logger.info("Showing first {} rows of table {}", limit:Any, tablename:Any)

        Try {
            val table = executor.instantiate(TableIdentifier.parse((tablename)))
            table.limit(limit).show(truncate = false)
        } match {
            case Success(_) =>
                logger.info("Successfully finished dumping table")
                true
            case Failure(e) =>
                logger.error("Caught exception while dumping table: {}", e)
                false
        }
    }
}
