package com.dimajix.flowman.tools.exec.model

import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.ActionCommand


class ListCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[ListCommand])

    override def executeInternal(session: Session, context:Context, project: Project) : Boolean = {
        project.relations.keys.foreach(println)
        true
    }

}
