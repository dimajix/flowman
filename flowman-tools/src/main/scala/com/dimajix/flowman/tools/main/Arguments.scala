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

package com.dimajix.flowman.tools.main

import java.io.PrintStream

import scala.collection.JavaConverters._

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option


class Arguments(args:Array[String]) {
    @Option(name = "-h", aliases=Array("--help"), usage = "show help", help=true)
    var _help: Boolean = false
    @Option(name = "-f", aliases=Array("--project"), usage = "project file or directory", metaVar = "<project_file>")
    var projectFile: String = "project.yml"
    @Option(name = "-b", aliases=Array("--bundle"), usage = "bundle to execute", metaVar = "<bundle>")
    var bundle: String = "main"
    @Option(name = "-P", aliases=Array("--profile"), usage = "activate profile with specified name", metaVar = "<profile>")
    var profiles: Array[String] = Array()
    @Option(name = "-D", aliases=Array("--env"), usage = "set environment variables which can be accessed inside config", metaVar = "<key=value>")
    var environment: Array[String] = Array()
    @Option(name = "--conf", usage = "set a Flowman or Spark config", metaVar = "<confname>=<value>")
    var config: Array[String] = Array()
    @Option(name = "--info", usage = "dump configuration information")
    var info: Boolean = false
    @Option(name = "--force", usage = "force execution even if targets already exist")
    var force: Boolean = false
    @Option(name = "--spark-logging", usage = "set the log level for Spark", metaVar = "<spark_logging>")
    var sparkLogging: String = "WARN"
    @Option(name = "--spark-name", usage = "set the Spark job name", metaVar = "<job_name>")
    var sparkName: String = "flowman"

    @Argument(index=0, required=false, usage="the command to execute", metaVar="command")
    var phase:String = ""
    @Argument(index=1, required=false, usage = "specifies project specific parameters", metaVar = "<param>=<value>")
    var arguments:Array[String] = Array()

    /**
      * Returns true if a help message is requested
      * @return
      */
    def help : Boolean = _help || phase.isEmpty

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
        try {
            parser.parseArgument(args.toList.asJava)
        }
        catch {
            case e: CmdLineException => {
                e.getParser.printUsage(System.err)
                System.err.println
                throw e
            }
        }
    }
}
