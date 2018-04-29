package com.dimajix.flowman.tools.exec.model

import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.tools.exec.ActionCommand


class ListCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[ListCommand])

    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        val context = executor.context
        project.relations.keys.foreach(println)
        true
    }

}
