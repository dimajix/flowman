/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman

import java.io.File
import java.nio.file.Path

import org.apache.hadoop.conf.Configuration

import com.dimajix.common.Resources
import com.dimajix.flowman.common.ToolConfig
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.fs.FileSystem
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.SystemSettings
import com.dimajix.flowman.plugin.PluginManager


object Tool {
    def resolvePath(path: String): com.dimajix.flowman.fs.File = {
        // Create Hadoop FileSystem instance
        val hadoopConfig = new Configuration()
        val fs = FileSystem(hadoopConfig)

        // Load Project. If no schema is specified, load from local file system
        val protocol = FileSystem.getProtocol(path)
        if (protocol.isEmpty) {
            // Try to load from resources folder in fat-jar
            val url = Resources.getURL("META-INF/flowman/" + path)
            if (url != null)
                fs.resource(url.toURI)
            else
                fs.local(path)
        }
        else {
            fs.file(path)
        }
    }

    def resolvePath(path: File): com.dimajix.flowman.fs.File = {
        // Create Hadoop FileSystem instance
        val hadoopConfig = new Configuration()
        val fs = FileSystem(hadoopConfig)

        fs.local(path)
    }
}

class Tool {
    // First create PluginManager
    final protected val pluginManager:PluginManager = createPluginManager()
    // Second load global system settings (including plugins for namespaces)
    final val systemSettings:SystemSettings = loadSystemSettings()
    // Third load namespace
    final val namespace:Namespace = loadNamespace()

    protected def createPluginManager() : PluginManager = {
        val pluginManager = PluginManager.builder()
        ToolConfig.pluginDirectory.foreach(pluginManager.withPluginDir)
        pluginManager.build()
    }

    protected def loadSystemSettings() : SystemSettings = {
        val settings = ToolConfig.confDirectory
            .map(confDir => new File(confDir, "system.yml"))
            .filter(_.isFile)
            .map(file => SystemSettings.read.file(file))
            .orElse(
                Option(Resources.getURL("META-INF/flowman/conf/system.yml"))
                    .map(SystemSettings.read.url)
            )
            .getOrElse(SystemSettings.read.default())

        // Load all global plugins from System settings
        settings.plugins.foreach(pluginManager.load)
        settings
    }

    protected def loadNamespace() : Namespace = {
        val ns = ToolConfig.confDirectory
            .map(confDir => new File(confDir, "default-namespace.yml"))
            .filter(_.isFile)
            .map(file => Namespace.read.file(file))
            .orElse(
                Option(Resources.getURL("META-INF/flowman/conf/default-namespace.yml"))
                    .map(Namespace.read.url)
            )
            .getOrElse(Namespace.read.default())

        // Load all plugins from Namespace
        ns.plugins.foreach(pluginManager.load)
        ns
    }

    protected def createSession(
         sparkMaster:String,
         sparkName:String,
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
            .withConfig(allConfigs)
            .withEnvironment(additionalEnvironment)
            .withProfiles(profiles)
            .withJars(pluginManager.jars.map(_.toString))

        project.foreach(builder.withProject)

        if (sparkName.nonEmpty)
            builder.withSparkName(sparkName)
        if (sparkMaster.nonEmpty)
            builder.withSparkMaster(sparkMaster)

        if (disableSpark)
            builder.disableSpark()
        else
            builder.enableSpark()

        builder.build()
    }
}
