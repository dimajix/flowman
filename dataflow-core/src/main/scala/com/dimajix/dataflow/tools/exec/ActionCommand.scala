package com.dimajix.dataflow.tools.exec

import org.kohsuke.args4j.Option

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.execution.Session
import com.dimajix.dataflow.spec.Project


abstract class ActionCommand extends Command {
    @Option(name = "-f", aliases=Array("--file"), usage = "project file", metaVar = "<project>")
    var projectFile: String = "project.yaml"

    override def execute(project:Project, session: Session): Boolean = {
        super.execute(project, session)

        val model = Project.read.file(projectFile)

        // Create project specific executor
        val executor = session.createExecutor(project)
        val result = executeInternal(executor, model)

        // Cleanup caches, but after printing error message. Otherwise it looks confusing when the error occured
        executor.cleanup()

        result
    }

    def executeInternal(executor:Executor, dataflow: Project) : Boolean
}
