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

import org.apache.hadoop.conf.Configuration
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.fs.FileSystem
import com.dimajix.flowman.namespace.Namespace
import com.dimajix.flowman.plugin.PluginManager
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

    private lazy val plugins:PluginManager = {
        val pluginDir = new File(System.getenv("FLOWMAN_HOME"), "plugins")
        new PluginManager().withPluginDir(pluginDir)
    }

    private def loadSystemPlugins() : Unit = {
        // TODO
    }

    private def loadNamespace() : Namespace = {
        val ns = Option(System.getenv("FLOWMAN_CONF_DIR"))
            .filter(_.nonEmpty)
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

        // Load Namespace (including any plugins), afterwards also load Project
        Project.read.file(fs.local(options.projectFile))
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

        // Check if only help is requested
        if (options.help) {
            options.printHelp(System.out)
            System.exit(0)
        }

        // Check if command is otherwise incomplete
        if (options.incomplete) {
            options.printHelp(System.err)
            System.exit(1)
        }

        // Load global Plugins, which can already be used by the Namespace
        loadSystemPlugins()

        // Load Namespace (including any plugins), afterwards also load Project
        val ns = loadNamespace()
        val project = loadProject()

        // Create Flowman Session, which also includes a Spark Session
        val sparkConfig = splitSettings(options.sparkConfig)
        val environment = splitSettings(options.environment)
        val session:Session = Session.builder
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
