package com.dimajix.dataflow.tools.dfexec.model

import org.kohsuke.args4j.Option

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Session
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.tools.dfexec.Arguments
import com.dimajix.dataflow.tools.dfexec.Command
import com.dimajix.dataflow.util.splitSettings


abstract class AbstractCommand extends Command {
    @Option(name = "-f", aliases=Array("--file"), usage = "dataflow file", metaVar = "<dataflow>")
    var dataflowFile: String = "model.yaml"

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
