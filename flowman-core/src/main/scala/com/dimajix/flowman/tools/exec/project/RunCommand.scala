package com.dimajix.flowman.tools.exec.project

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.tools.exec.ActionCommand


class RunCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[RunCommand])

    @Argument(usage = "specifies outputs to process", metaVar = "<output>")
    var outputs: Array[String] = Array()


    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        implicit val context = executor.context
        false
    }
}
