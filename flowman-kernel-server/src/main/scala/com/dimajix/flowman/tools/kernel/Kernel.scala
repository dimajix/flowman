/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.flowman.tools.kernel

import java.io.File

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.CmdLineException
import org.slf4j.LoggerFactory

import com.dimajix.flowman.FLOWMAN_VERSION
import com.dimajix.flowman.HADOOP_BUILD_VERSION
import com.dimajix.flowman.JAVA_VERSION
import com.dimajix.flowman.SCALA_VERSION
import com.dimajix.flowman.SPARK_BUILD_VERSION
import com.dimajix.flowman.Tool
import com.dimajix.flowman.common.ConsoleColors.yellow
import com.dimajix.flowman.common.Logging
import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.common.ToolConfig
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.kernel.KernelServer
import com.dimajix.flowman.kernel.grpc.KernelServiceHandler
import com.dimajix.flowman.kernel.service.MultiWorkspaceManager
import com.dimajix.flowman.kernel.service.WorkspaceManager
import com.dimajix.hadoop.HADOOP_VERSION
import com.dimajix.spark.SPARK_VERSION
import com.dimajix.spark.features.isDatabricks



object Kernel {
    def main(args: Array[String]) : Unit = {
        Logging.init()

        Try {
            run(args:_*)
        }
        match {
            case Success (true) =>
                // Successful without error should not use System.exit()
                //System.exit(0)
            case Success (false) =>
                System.exit(3)
            case Failure(ex:CmdLineException) =>
                System.err.println(ex.getMessage)
                ex.getParser.printUsage(System.err)
                System.err.println
                System.exit(4)
            case Failure(exception) =>
                exception.printStackTrace(System.err)
                System.exit(5)
        }
    }

    def run(args: String*) : Boolean = {
        val options = new Arguments(args.toArray)
        // Check if only help or version is requested
        if (options.version) {
            println(s"Flowman $FLOWMAN_VERSION")
            println(s"Flowman home: ${ToolConfig.homeDirectory.getOrElse("<unknown>")}")
            println(s"Spark version $SPARK_VERSION")
            println(s"Hadoop version $HADOOP_VERSION")
            println(s"Scala version $SCALA_VERSION")
            println(s"Java version $JAVA_VERSION")
            true
        }
        else if (options.help) {
            options.printHelp(System.out)
            true
        }
        else {
            if (options.infoLogging) {
                Logging.setLogging("INFO")
            }
            if (options.debugLogging) {
                Logging.setLogging("DEBUG")
            }
            Logging.setSparkLogging(options.sparkLogging)

            val kernel = new Kernel(options)
            kernel.start()
            kernel.awaitTermination()

            true
        }
    }
}


final class Kernel(args:Arguments) extends Tool {
    lazy val workspaceDirectory =
        if (args.workspace.isEmpty)
            Tool.resolvePath(new File(".").getAbsoluteFile)
        else
            Tool.resolvePath(args.workspace)
    lazy val workspaceManager: WorkspaceManager = {
        new MultiWorkspaceManager(workspaceDirectory)
    }
    lazy val rootSession: Session = createSession(
        args.sparkMaster,
        args.sparkName,
        additionalConfigs = splitSettings(args.config).toMap
    )
    lazy val server: KernelServer = KernelServer.builder(rootSession, pluginManager)
        .withWorkspaceManager(workspaceManager)
        .withPort(args.port)
        .withSocketServer()
        .build()

    /**
     * Main method for running this command
     *
     * @return
     */
    def start(): Unit = {
        val logo =
            """______  _
              ||  ___|| |
              || |_   | |  ___ __      __ _ __ ___    __ _  _ __
              ||  _|  | | / _ \\ \ /\ / /| '_ ` _ \  / _` || '_ \
              || |    | || (_) |\ V  V / | | | | | || (_| || | | |
              |\_|    |_| \___/  \_/\_/  |_| |_| |_| \__,_||_| |_|""".stripMargin
        logger.info(s"\n$logo    $FLOWMAN_VERSION\n")

        logger.info(s"Flowman $FLOWMAN_VERSION using Spark $SPARK_VERSION and Hadoop $HADOOP_VERSION and Scala $SCALA_VERSION (Java $JAVA_VERSION)")
        if (SPARK_VERSION != SPARK_BUILD_VERSION || HADOOP_VERSION != HADOOP_BUILD_VERSION) {
            logger.warn(yellow("Detected Version mismatch between build and execution:"))
            logger.warn(yellow(s"  Hadoop build version: ${HADOOP_BUILD_VERSION}, Hadoop execution version: ${HADOOP_VERSION}"))
            logger.warn(yellow(s"  Spark build version: ${SPARK_BUILD_VERSION}, Spark execution version: ${SPARK_VERSION}"))
            logger.warn(yellow("It is highly recommended to use matching versions, specifically for Spark."))
        }

        server.start()
        val sockets = server.getListenSockets()
        logger.info(s"Started Flowman kernel, listening for incoming connections at ${sockets.mkString(", ")}")
    }

    def shutdown(): Unit = {
        if (!server.isTerminated()) {
            server.stop()

            // github-357: Spark session should not be shut down in DataBricks environment
            if (!isDatabricks)
                rootSession.shutdown()
        }
    }

    def awaitTermination() : Unit = {
        server.awaitTermination()
    }
}
