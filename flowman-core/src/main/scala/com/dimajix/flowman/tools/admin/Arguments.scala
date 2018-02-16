package com.dimajix.flowman.tools.dfadmin

import scala.collection.JavaConversions._

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands


class Arguments(args:Array[String]) extends NestedCommand {
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
        //new SubCommand(name="namespace",impl=classOf[NamespaceCommand])
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
