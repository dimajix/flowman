package com.dimajix.flowman.tools.exec

import org.kohsuke.args4j.Option

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Project


abstract class ActionCommand extends Command {
    override def execute(project:Project, session: Session): Boolean = {
        super.execute(project, session)

        // Create project specific executor
        val executor = session.createExecutor(project)
        val result = executeInternal(executor, project)

        // Cleanup caches, but after printing error message. Otherwise it looks confusing when the error occured
        executor.cleanup()

        result
    }

    def executeInternal(executor:Executor, project: Project) : Boolean
}
