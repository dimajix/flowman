package com.dimajix.flowman.tools.exec

import scala.collection.JavaConversions._

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.flowman.tools.exec.mapping.MappingCommand
import com.dimajix.flowman.tools.exec.model.ModelCommand
import com.dimajix.flowman.tools.exec.output.OutputCommand
import com.dimajix.flowman.tools.exec.project.ProjectCommand


class Arguments(args:Array[String]) {
    @Option(name = "-h", aliases=Array("--help"), usage = "show help")
    var help: Boolean = false
    @Option(name = "-f", aliases=Array("--project"), usage = "project file", metaVar = "<project_file>")
    var projectFile: String = "project.yml"
    @Option(name = "-p", aliases=Array("--profile"), usage = "profile to enable", metaVar = "<profile>")
    var profiles: Array[String] = Array()
    @Option(name = "-e", aliases=Array("--env"), usage = "sets environment variables which can be accessed inside config", metaVar = "<key=value>")
    var environment: Array[String] = Array()
    @Option(name = "--info", usage = "dump configuration information")
    var info: Boolean = false
    @Option(name = "--spark-logging", usage = "sets the log level for Spark", metaVar = "<spark_logging>")
    var sparkLogging: String = "WARN"
    @Option(name = "--spark-name", usage = "sets the Spark job name", metaVar = "<job_name>")
    var sparkName: String = "flowman"
    @Option(name = "--spark-conf", usage = "sets a Spark config", metaVar = "<confname>=<value>")
    var sparkConfig: Array[String] = Array()

    @Argument(required=false,index=0,metaVar="group",usage="the object to work with",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="model",impl=classOf[ModelCommand]),
        new SubCommand(name="mapping",impl=classOf[MappingCommand]),
        new SubCommand(name="output",impl=classOf[OutputCommand]),
        new SubCommand(name="project",impl=classOf[ProjectCommand])
    ))
    var command:Command = _

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
}
