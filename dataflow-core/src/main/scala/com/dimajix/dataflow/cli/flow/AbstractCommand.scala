package com.dimajix.dataflow.cli.flow

import org.kohsuke.args4j.Option

import com.dimajix.dataflow.cli.Arguments
import com.dimajix.dataflow.cli.Command
import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Session
import com.dimajix.dataflow.spec.Dataflow
import com.dimajix.dataflow.util.splitSettings


abstract class AbstractCommand extends Command {
    @Option(name = "-f", aliases=Array("--file"), usage = "dataflow file", metaVar = "<dataflow>")
    var dataflowFile: String = "dataflow.yaml"

    override def execute(options:Arguments): Boolean = {
        super.execute(options)

        val dataflow = Dataflow.load(dataflowFile)

        val sparkConfig = splitSettings(options.sparkConfig)
        val environment = splitSettings(options.environment)
        val session = new Session(options.sparkName, sparkConfig, environment)

        val context = session.createContext(dataflow, options.profiles)
        val result = executeInternal(context, dataflow)
        context.cleanup()

        result
    }

    def executeInternal(context:Context, dataflow: Dataflow) : Boolean
}
