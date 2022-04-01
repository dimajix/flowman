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

package com.dimajix.flowman.tools.schema

import java.io.PrintStream

import scala.collection.JavaConverters._

import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option


class Arguments(args:Array[String]) {
    @Option(name = "-h", aliases=Array("--help"), usage = "show help", help=true)
    var _help: Boolean = false
    @Option(name = "-v", aliases=Array("--version"), usage = "show version", help=true)
    var version: Boolean = false
    @Option(name = "-o", aliases=Array("--output"), usage = "otuput file", help=true)
    var output: String = ""

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
