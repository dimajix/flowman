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

package com.dimajix.flowman.tools

import java.io.File
import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory

import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.plugin.PluginManager
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.SystemSettings
import com.dimajix.flowman.tools.exec.Driver


class Tool {
    private val logger = LoggerFactory.getLogger(classOf[Driver])

    // First create PluginManager
    val plugins:PluginManager = createPluginManager()
    // Second load global system settings (including plugins for namespaces)
    val systemSettings:SystemSettings = loadSystemSettings()
    // Third load namespace
    val namespace:Namespace = loadNamespace()

    private def createPluginManager() : PluginManager = {
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

    def loadProject(projectPath:Path) : Project = {
        // Create Hadoop FileSystem instance
        val hadoopConfig = new Configuration()
        val fs = FileSystem(hadoopConfig)

        // Load Project. If no schema is specified, load from local file system
        val projectUri = projectPath.toUri
        if (projectUri.getAuthority == null && projectUri.getScheme == null)
            Project.read.file(fs.local(projectPath))
        else
            Project.read.file(fs.file(projectPath))
    }

    def createSession(
        name:String,
        project:Option[Project]=None,
        additionalEnvironment:Map[String,String] = Map(),
        additionalConfigs:Map[String,String] = Map(),
        profiles:Seq[String] = Seq(),
        disableSpark:Boolean = false
    ) : Session = {
        // Enrich Flowman configuration by directories
        val allConfigs =
            ToolConfig.homeDirectory.map(FlowmanConf.HOME_DIRECTORY.key -> _.toString).toMap ++
                ToolConfig.confDirectory.map(FlowmanConf.CONF_DIRECTORY.key -> _.toString).toMap ++
                ToolConfig.pluginDirectory.map(FlowmanConf.PLUGIN_DIRECTORY.key -> _.toString).toMap ++
                additionalConfigs

        // Create Flowman Session, which also includes a Spark Session
        val builder = Session.builder()
            .withNamespace(namespace)
            .withProject(project.orNull)
            .withSparkName(name)
            .withConfig(allConfigs)
            .withEnvironment(additionalEnvironment)
            .withProfiles(profiles)
            .withJars(plugins.jars.map(_.toString))

        if (disableSpark)
            builder.disableSpark()

        builder.build()
    }

    def setupLogging(sparkLogging:Option[String]) : Unit = {
        val log4j = System.getProperty("log4j.configuration")
        if (log4j == null || log4j.isEmpty) {
            val loader = Thread.currentThread.getContextClassLoader
            val url = loader.getResource("com/dimajix/flowman/log4j-defaults.properties")
            PropertyConfigurator.configure(url)
            logger.debug(s"Loaded logging configuration from $url")
        }

        // Adjust Spark logging level
        sparkLogging.foreach { level =>
            logger.debug(s"Setting Spark log level to ${level}")
            val upperCased = level.toUpperCase(Locale.ENGLISH)
            val l = org.apache.log4j.Level.toLevel(upperCased)
            org.apache.log4j.Logger.getLogger("org").setLevel(l)
            org.apache.log4j.Logger.getLogger("akka").setLevel(l)
            org.apache.log4j.Logger.getLogger("hive").setLevel(l)
        }
    }
}
