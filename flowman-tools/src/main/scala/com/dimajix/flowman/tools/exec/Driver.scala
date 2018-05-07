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

package com.dimajix.flowman.tools.exec

import java.io.File
import java.util.Locale

import org.apache.log4j.PropertyConfigurator
import org.kohsuke.args4j.CmdLineParser
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.util.splitSettings


object Driver {
    def main(args: Array[String]) : Unit = {
        // First create driver, so can already process arguments
        val options = new Arguments(args)
        val driver = new Driver(options)

        val result = driver.run()
        System.exit(if (result) 0 else 1)
    }
}


class Driver(options:Arguments) {
    private val logger = LoggerFactory.getLogger(classOf[Driver])

    private def loadNamespace() : Namespace = {
        Option(System.getenv("FLOWMAN_CONF_DIR"))
            .filter(_.nonEmpty)
            .map(confDir => new File(confDir, "default-namespace.yml"))
            .filter(_.isFile)
            .map(file => Namespace.read.file(file))
            .getOrElse(Namespace.read.default())
    }

    /**
      * Main method for running this command
      * @return
      */
    def run() : Boolean = {
        val log4j = System.getProperty("log4j.configuration")
        if (log4j == null || log4j.isEmpty) {
            val loader = Thread.currentThread.getContextClassLoader
            val url = loader.getResource("com/dimajix/flowman/log4j-defaults.properties")
            PropertyConfigurator.configure(url)
            logger.debug(s"Loaded Logging configuration from $url")
        }

        // Adjust Sgpark loglevel
        if (options.sparkLogging != null) {
            logger.debug(s"Setting Spark debug level to ${options.sparkLogging}")
            val upperCased = options.sparkLogging.toUpperCase(Locale.ENGLISH)
            val l = org.apache.log4j.Level.toLevel(upperCased)
            org.apache.log4j.Logger.getLogger("org").setLevel(l)
            org.apache.log4j.Logger.getLogger("akka").setLevel(l)
            org.apache.log4j.Logger.getLogger("hive").setLevel(l)
        }

        if (options.help || options.command == null) {
            new CmdLineParser(if (options.command != null) options.command else options).printUsage(System.err)
            System.err.println
            System.exit(1)
        }

        val project:Project = Project.read.file(options.projectFile)

        val sparkConfig = splitSettings(options.sparkConfig)
        val environment = splitSettings(options.environment)
        val session:Session = Session.builder
            .withNamespace(loadNamespace())
            .withProject(project)
            .withSparkName(options.sparkName)
            .withSparkConfig(sparkConfig.toMap)
            .withEnvironment(environment.toMap)
            .withProfiles(options.profiles)
            .build()

        options.command.execute(project, session)
    }
}
