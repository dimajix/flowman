package com.dimajix.dataflow.tools.dfexec

import org.kohsuke.args4j.Option

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Session
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.util.splitSettings


abstract class ActionCommand extends Command {
    @Option(name = "-f", aliases=Array("--file"), usage = "project file", metaVar = "<project>")
    var dataflowFile: String = "project.yaml"

    override def execute(options:Arguments): Boolean = {
        super.execute(options)

        val model = Project.load(dataflowFile)

        val sparkConfig = splitSettings(options.sparkConfig)
        val environment = splitSettings(options.environment)
        val session = new Session(options.sparkName, sparkConfig, environment)

        val context = session.createContext(model, options.profiles)
        val result = executeInternal(context, model)
        context.cleanup()

        result
    }

    def executeInternal(context:Context, dataflow: Project) : Boolean
}
