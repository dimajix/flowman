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

import java.net.URI

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.apache.http.impl.client.HttpClients
import org.kohsuke.args4j.CmdLineException
import org.slf4j.LoggerFactory

import com.dimajix.flowman.common.Logging
import com.dimajix.flowman.common.ToolConfig


object Client {
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
        // Check if only help or version is requested
        if (options.version) {
            println(s"Flowman $FLOWMAN_VERSION")
            println(s"Flowman home: ${ToolConfig.homeDirectory.getOrElse("<unknown>")}")
            println(s"Spark version $SPARK_BUILD_VERSION")
            println(s"Hadoop version $HADOOP_BUILD_VERSION")
            println(s"Java version $JAVA_VERSION")
            println(s"Scala version $SCALA_VERSION")
            true
        }
        else if (options.help) {
            options.printHelp(System.out)
            true
        }
        else {
            val driver = new Client(options)
            driver.run()
        }
    }
}


class Client(options:Arguments) {
    private val logger = LoggerFactory.getLogger(classOf[Client])

    /**
     * Main method for running this command
     * @return
     */
    def run() : Boolean = {
        val command = options.command
        if (command.help) {
            command.printHelp(System.out)
            true
        }
        else {
            val baseUri = new URI(options.server)
            val httpClient = HttpClients.createSystem()
            try {
                options.command.execute(httpClient, baseUri)
            }
            finally {
                httpClient.close()
            }
        }
    }
}
