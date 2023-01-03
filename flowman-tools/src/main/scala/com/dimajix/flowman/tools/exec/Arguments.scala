/*
 * Copyright 2018-2022 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.tools.exec

import java.io.PrintStream

import scala.collection.JavaConverters._

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.flowman.tools.exec.documentation.DocumentationCommand
import com.dimajix.flowman.tools.exec.history.HistoryCommand
import com.dimajix.flowman.tools.exec.info.InfoCommand
import com.dimajix.flowman.tools.exec.job.JobCommand
import com.dimajix.flowman.tools.exec.mapping.MappingCommand
import com.dimajix.flowman.tools.exec.relation.ModelCommand
import com.dimajix.flowman.tools.exec.namespace.NamespaceCommand
import com.dimajix.flowman.tools.exec.project.ProjectCommand
import com.dimajix.flowman.tools.exec.target.TargetCommand
import com.dimajix.flowman.tools.exec.test.TestCommand


class Arguments(args:Array[String]) {
    @Option(name = "-h", aliases=Array("--help"), usage = "show help", help=true)
    var _help: Boolean = false
    @Option(name = "-v", aliases=Array("--version"), usage = "show version", help=true)
    var version: Boolean = false
    @Option(name = "-f", aliases=Array("--project"), usage = "project file or directory", metaVar = "<project_file>")
    var projectFile: String = "project.yml"
    @Option(name = "-P", aliases=Array("--profile"), usage = "activate profile with specified name", metaVar = "<profile>")
    var profiles: Array[String] = Array()
    @Option(name = "-D", aliases=Array("--env"), usage = "set environment variables which can be accessed inside config", metaVar = "<key=value>")
    var environment: Array[String] = Array()
    @Option(name = "-B", aliases=Array("--batch-mode"), usage = "Run in non-interactive batch mode. Disables output color.")
    var batchMode: Boolean = false
    @Option(name = "-X", aliases = Array("--verbose"), usage = "Produce execution output at info level.")
    var infoLogging: Boolean = false
    @Option(name = "-XX", aliases = Array("--debug"), usage = "Produce execution output at debug level.")
    var debugLogging: Boolean = false
    @Option(name = "--conf", usage = "set a Flowman or Spark config", metaVar = "<confname>=<value>")
    var config: Array[String] = Array()
    @Option(name = "--info", usage = "dump configuration information")
    var info: Boolean = false
    @Option(name = "--spark-master", usage = "set the master for Spark", metaVar = "<spark_master>")
    var sparkMaster: String = ""
    @Option(name = "--spark-logging", usage = "set the log level for Spark", metaVar = "<spark_logging>")
    var sparkLogging: String = "WARN"
    @Option(name = "--spark-name", usage = "set the Spark application name", metaVar = "<spark_application_name>")
    var sparkName: String = ""

    @Argument(required=false,index=0,metaVar="<command-group>",usage="the object to work with",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="documentation",impl=classOf[DocumentationCommand]),
        new SubCommand(name="history",impl=classOf[HistoryCommand]),
        new SubCommand(name="info",impl=classOf[InfoCommand]),
        new SubCommand(name="job",impl=classOf[JobCommand]),
        new SubCommand(name="model",impl=classOf[ModelCommand]),
        new SubCommand(name="mapping",impl=classOf[MappingCommand]),
        new SubCommand(name="namespace",impl=classOf[NamespaceCommand]),
        new SubCommand(name="project",impl=classOf[ProjectCommand]),
        new SubCommand(name="relation",impl=classOf[ModelCommand]),
        new SubCommand(name="target",impl=classOf[TargetCommand]),
        new SubCommand(name="test",impl=classOf[TestCommand]),
        new SubCommand(name="version",impl=classOf[VersionCommand])
    ))
    var command:Command = _

    /**
      * Returns true if a help message is requested
      * @return
      */
    def help : Boolean = _help || command == null || command.help

    /**
      * Prints a context-aware help message
      */
    def printHelp(out:PrintStream = System.err) : Unit = {
        if (command != null) {
            command.printHelp(out)
        }
        else {
            new CmdLineParser(this).printUsage(out)
            out.println
        }
    }

    parseArgs(args)

    private def parseArgs(args: Array[String]) {
        val parser: CmdLineParser = new CmdLineParser(this)
        parser.parseArgument(args.toList.asJava)
    }
}
