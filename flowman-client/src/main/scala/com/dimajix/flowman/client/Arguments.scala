/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.client

import java.io.PrintStream

import scala.collection.JavaConverters._

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.kohsuke.args4j.spi.SubCommand
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands

import com.dimajix.flowman.client.job.JobCommand
import com.dimajix.flowman.client.parcel.ParcelCommand
import com.dimajix.flowman.client.project.ProjectCommand
import com.dimajix.flowman.client.workspace.WorkspaceCommand


class Arguments(args:Array[String]) {
    @Option(name = "-h", aliases=Array("--help"), usage = "show help", help=true)
    var _help: Boolean = false
    @Option(name = "-v", aliases=Array("--version"), usage = "show version", help=true)
    var version: Boolean = false
    @Option(name = "-s", aliases=Array("--server"), usage = "URL of Flowman Studio server", help=true)
    var server: String = scala.Option(System.getenv("FLOWMAN_STUDIO_URL"))
        .orElse(Some("http://localhost:8080/api"))
        .map(uri => if (!uri.endsWith("/")) uri + "/" else uri).get

    @Argument(required=false,index=0,metaVar="<command-group>",usage="the object to work with",handler=classOf[SubCommandHandler])
    @SubCommands(Array(
        new SubCommand(name="workspace",impl=classOf[WorkspaceCommand]),
        new SubCommand(name="parcel",impl=classOf[ParcelCommand]),
        new SubCommand(name="project",impl=classOf[ProjectCommand]),
        new SubCommand(name="job",impl=classOf[JobCommand]),
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
