/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.tools.Tool
import com.dimajix.flowman.tools.exec.job.PhaseCommand


object Driver {
    def main(args: Array[String]) : Unit = {
        Try {
            val options = new Arguments(args)

            // Check if only help is requested
            if (options.help) {
                options.printHelp(System.out)
                true
            }

            else {
                val driver = new Driver(options)
                driver.run()
            }
        }
        match {
            case Success (true) => System.exit(0)
            case Success (false) => System.exit(1)
            case Failure(exception) => System.err.println(exception.getMessage)
        }
    }
}


class Driver(options:Arguments) extends Tool {
    private val logger = LoggerFactory.getLogger(classOf[Driver])

    /**
      * Main method for running this command
      * @return
      */
    def run() : Boolean = {
        setupLogging(Option(options.sparkLogging))

        val project = loadProject(new Path(options.projectFile))

        // Create Flowman Session, which also includes a Spark Session
        val config = splitSettings(options.config)
        val environment = splitSettings(options.environment)
        val session = createSession(options.sparkName,
            project = Some(project),
            additionalConfigs = config.toMap,
            additionalEnvironment = environment.toMap,
            profiles = options.profiles
        )

        val executor = session.executor
        val context = session.getContext(project)

        //val bundle = context.getJob()
        //val bundleDescription = bundle.description.map("(" + _ + ")").getOrElse("")
        //val bundleArgs = options.arguments.map(kv => kv._1 + "=" + kv._2).mkString(", ")
        //logger.info(s"Executing job '${bundle.name}' $bundleDescription with args $bundleArgs")

        // val result = executeInternal(executor, context, project)


        // Cleanup caches, but after printing error message. Otherwise it looks confusing when the error occured
        executor.cleanup()
        true
    }
}
