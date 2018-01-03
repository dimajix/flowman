package com.dimajix.dataflow.tools.dfexec

import scala.collection.JavaConversions._

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.dataflow.tools.dfexec.flow.DataflowCommand
import com.dimajix.dataflow.tools.dfexec.model.ModelCommand


class Arguments(args:Array[String]) extends NestedCommand {
    @Option(name = "-c", aliases=Array("--config"), usage = "configuration file", metaVar = "<configfile>")
    var configFile: String = "dataflow.yaml"
    @Option(name = "-p", aliases=Array("--profile"), usage = "profile to enable", metaVar = "<profile>")
    var profiles: Array[String] = Array()
    @Option(name = "-e", aliases=Array("--env"), usage = "sets environment variables which can be accessed inside config", metaVar = "<key=value>")
    var environment: Array[String] = Array()
    @Option(name = "--info", usage = "dump configuration information")
    var info: Boolean = false
    @Option(name = "--spark-logging", usage = "sets the log level for Spark", metaVar = "<spark_logging>")
    var sparkLogging: String = "WARN"
    @Option(name = "--spark-name", usage = "sets the Spark job name", metaVar = "<job_name>")
    var sparkName: String = "datatool"
    @Option(name = "--spark-conf", usage = "sets a Spark config", metaVar = "<confname>=<value>")
    var sparkConfig: Array[String] = Array()

    @Argument(required=false,index=0,metaVar="group",usage="the object to work with",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="model",impl=classOf[ModelCommand]),
        new SubCommand(name="dataflow",impl=classOf[DataflowCommand])
    ))
    override var command:Command = _

    parseArgs(args)

    private def parseArgs(args: Array[String]) {
        val parser: CmdLineParser = new CmdLineParser(this)
        try {
            parser.parseArgument(args.toList)
        }
        catch {
            case e: CmdLineException => {
                System.err.println(e.getMessage)
                e.getParser.printUsage(System.err)
                System.err.println
                System.exit(1)
            }
        }
    }

    override def execute(options: Arguments): Boolean = {
        super.execute(options)

        command.execute(options)
    }
}
