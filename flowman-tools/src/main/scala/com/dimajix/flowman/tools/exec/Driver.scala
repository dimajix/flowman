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

package com.dimajix.flowman.tools.exec

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.tools.Logging
import com.dimajix.flowman.tools.Tool


object Driver {
    def main(args: Array[String]) : Unit = {
        Try {
            run(args:_*)
        }
        match {
            case Success (true) =>
                System.exit(0)
            case Success (false) =>
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
            Logging.setup(Option(options.sparkLogging))

            val driver = new Driver(options)
            driver.run()
        }
    }
}


class Driver(options:Arguments) extends Tool {
    /**
      * Main method for running this command
      * @return
      */
    def run() : Boolean = {
        val project = loadProject(new Path(options.projectFile))

        // Create Flowman Session, which also includes a Spark Session
        val config = splitSettings(options.config)
        val environment = splitSettings(options.environment)
        val session = createSession(
            options.sparkMaster,
            options.sparkName,
            project = Some(project),
            additionalConfigs = config.toMap,
            additionalEnvironment = environment.toMap,
            profiles = options.profiles
        )

        options.command.execute(project, session)
    }
}
