/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.kernel

import java.io.PrintStream
import java.net.URL

import scala.collection.JavaConverters._

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.flowman.tools.exec.info.InfoCommand
import com.dimajix.flowman.tools.exec.job.JobCommand
import com.dimajix.flowman.tools.exec.mapping.MappingCommand
import com.dimajix.flowman.tools.exec.model.ModelCommand
import com.dimajix.flowman.tools.exec.namespace.NamespaceCommand
import com.dimajix.flowman.tools.exec.project.ProjectCommand
import com.dimajix.flowman.tools.exec.target.TargetCommand
import com.dimajix.flowman.tools.exec.test.TestCommand


class Arguments(args:Array[String]) {
    @Option(name = "-h", aliases=Array("--help"), usage = "show help", help=true)
    var _help: Boolean = false
    @Option(name = "-P", aliases=Array("--profile"), usage = "activate profile with specified name", metaVar = "<profile>")
    var profiles: Array[String] = Array()
    @Option(name = "-D", aliases=Array("--env"), usage = "set environment variables which can be accessed inside config", metaVar = "<key=value>")
    var environment: Array[String] = Array()
    @Option(name = "--conf", usage = "set a Flowman or Spark config", metaVar = "<confname>=<value>")
    var config: Array[String] = Array()
    @Option(name = "--bind-host", usage = "set the host to bind the REST API to", metaVar = "<bind_host>")
    var bindHost: String = "0.0.0.0"
    @Option(name = "--bind-port", usage = "set the port to bind the REST API to. Use 0 for random port.", metaVar = "<bind_port>")
    var bindPort: Int = 8080
    @Option(name = "--kernel-id", usage = "set the kernel ID used for registration", metaVar = "<kernel_id>")
    var kernelId: String = ""
    @Option(name = "--kernel-secret", usage = "set the secret to use for communication with the Flowman Studio server", metaVar = "<studio_secret>")
    var kernelSecret: String = ""
    @Option(name = "--studio-url", usage = "set the URL to register to", metaVar = "<studio_url>")
    var studioUrl: String = ""
    @Option(name = "--spark-master", usage = "set the master for Spark", metaVar = "<spark_master>")
    var sparkMaster: String = ""
    @Option(name = "--spark-logging", usage = "set the log level for Spark", metaVar = "<spark_logging>")
    var sparkLogging: String = "WARN"
    @Option(name = "--spark-name", usage = "set the Spark application name", metaVar = "<spark_application_name>")
    var sparkName: String = "Flowman Kernel"

    /**
      * Returns true if a help message is requested
      * @return
      */
    def help : Boolean = _help

    /**
      * Prints a context-aware help message
      */
    def printHelp(out:PrintStream = System.err) : Unit = {
        new CmdLineParser(this).printUsage(out)
        out.println
    }

    parseArgs(args)

    private def parseArgs(args: Array[String]) {
        val parser: CmdLineParser = new CmdLineParser(this)
        parser.parseArgument(args.toList.asJava)
    }
}
