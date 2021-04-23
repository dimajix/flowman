package com.dimajix.flowman.tools.exec.model

import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command


class ListCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[ListCommand])

    override def execute(session: Session, project: Project, context:Context) : Boolean = {
        project.relations.keys.toList.sorted.foreach(println)
        true
    }

}
