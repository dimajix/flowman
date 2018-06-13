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

package com.dimajix.flowman.server.master

import scala.collection.JavaConversions._

import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option


class Arguments(args:Array[String]) {
    @Option(name = "-h", aliases=Array("--help"), usage = "show help")
    var help: Boolean = false
    @Option(name = "--host", usage = "Host to bind to")
    var bindHost: String = "0.0.0.0"
    @Option(name = "--port", usage = "Port to bind to")
    var bindPort: Int = 8080

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
