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

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.plugin.PluginManager
import com.dimajix.flowman.spec.SystemSettings
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.tools.ToolConfig


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


class Driver(options:Arguments) {
    private val logger = LoggerFactory.getLogger(classOf[Driver])

    private lazy val plugins:PluginManager = {
        val pluginManager = new PluginManager
        ToolConfig.pluginDirectory.foreach(pluginManager.withPluginDir)
        pluginManager
    }

    private def loadSystemSettings() : SystemSettings = {
        val settings = ToolConfig.confDirectory
            .map(confDir => new File(confDir, "system.yml"))
            .filter(_.isFile)
            .map(file => SystemSettings.read.file(file))
            .getOrElse(SystemSettings.read.default())

        // Load all global plugins from System settings
        settings.plugins.foreach(plugins.load)
        settings
    }

    private def loadNamespace() : Namespace = {
        val ns = ToolConfig.confDirectory
            .map(confDir => new File(confDir, "default-namespace.yml"))
            .filter(_.isFile)
            .map(file => Namespace.read.file(file))
            .getOrElse(Namespace.read.default())

        // Load all plugins from Namespace
        ns.plugins.foreach(plugins.load)
        ns
    }

    private def loadProject() : Project = {
        // Create Hadoop FileSystem instance
        val hadoopConfig = new Configuration()
        val fs = FileSystem(hadoopConfig)

        // Load Project
        Project.read.file(fs.local(options.projectFile))
    }

    private def setupLogging() : Unit = {
        val log4j = System.getProperty("log4j.configuration")
        if (log4j == null || log4j.isEmpty) {
            val loader = Thread.currentThread.getContextClassLoader
            val url = loader.getResource("com/dimajix/flowman/log4j-defaults.properties")
            PropertyConfigurator.configure(url)
            logger.debug(s"Loaded Logging configuration from $url")
        }

        // Adjust Spark logging level
        if (options.sparkLogging != null) {
            logger.debug(s"Setting Spark debug level to ${options.sparkLogging}")
            val upperCased = options.sparkLogging.toUpperCase(Locale.ENGLISH)
            val l = org.apache.log4j.Level.toLevel(upperCased)
            org.apache.log4j.Logger.getLogger("org").setLevel(l)
            org.apache.log4j.Logger.getLogger("akka").setLevel(l)
            org.apache.log4j.Logger.getLogger("hive").setLevel(l)
        }
    }

    /**
      * Main method for running this command
      * @return
      */
    def run() : Boolean = {
        setupLogging()

        // Load global Plugins, which can already be used by the Namespace
        loadSystemSettings()

        // Load Namespace (including any plugins), afterwards also load Project
        val ns = loadNamespace()
        val project = loadProject()

        // Create Flowman Session, which also includes a Spark Session
        val sparkConfig = splitSettings(options.sparkConfig)
        val environment = splitSettings(options.environment)
        val session = Session.builder
            .withNamespace(ns)
            .withProject(project)
            .withSparkName(options.sparkName)
            .withSparkConfig(sparkConfig.toMap)
            .withEnvironment(environment)
            .withProfiles(options.profiles)
            .withJars(plugins.jars.map(_.toString))
            .build()

        options.command.execute(project, session)
    }
}
