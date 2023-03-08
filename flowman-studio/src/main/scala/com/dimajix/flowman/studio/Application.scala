/*
 * Copyright (C) 2019 The Flowman Authors
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

package com.dimajix.flowman.studio

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.CmdLineException
import com.dimajix.flowman.Tool

import com.dimajix.flowman.common.Logging
import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.studio.rest.Server


object Application {
    def main(args: Array[String]) : Unit = {
        Logging.init()
        Try {
            run(args:_*)
        }
        match {
            case Success (true) =>
                System.exit(0)
            case Success (false) =>
                System.exit(1)
            case Failure(ex:CmdLineException) =>
                System.err.println(ex.getMessage)
                ex.getParser.printUsage(System.err)
                System.err.println
                System.exit(1)
            case Failure(exception) =>
                exception.printStackTrace(System.err)
                System.exit(1)
        }
    }

    def run(args: String*) : Boolean = {
        val options = new Arguments(args.toArray)
        // Check if only help is requested
        if (options.help) {
            options.printHelp(System.out)
            true
        }
        else {
            Logging.setSparkLogging(options.sparkLogging)

            val app = new Application(options)
            app.run()
        }
    }
}


class Application(options:Arguments) extends Tool {
    def run() : Boolean = {
        val config = splitSettings(options.config)
        val environment = splitSettings(options.environment)
        val session = createSession(
            options.sparkMaster,
            options.sparkName,
            additionalConfigs = config.toMap,
            additionalEnvironment = environment.toMap,
            profiles = options.profiles
        )

        val conf = Configuration.loadDefaults()
            .setWorkspaceRoot(options.workspaceRoot)
            .setBindHost(options.bindHost)
            .setBindPort(options.bindPort)
            .setStudioId(options.studioId)
            .setHubUrl(options.hubUrl)
            .setHubSecret(options.hubSecret)
        val server = new Server(conf, session)
        server.run()

        session.shutdown()

        true
    }
}
