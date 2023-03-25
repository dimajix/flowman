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

import scala.util.control.NonFatal
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.common.Resources
import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.common.ToolConfig
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.fs.FileSystem
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.SystemSettings
import com.dimajix.flowman.plugin.PluginManager
import com.dimajix.flowman.util.ObjectMapper


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
    protected final val logger = LoggerFactory.getLogger(getClass)
    // First create PluginManager
    protected final val pluginManager:PluginManager = createPluginManager()
    // Second load global system settings (including plugins for namespaces)
    final val systemSettings:SystemSettings = loadSystemSettings()
    // Third load namespace
    final val namespace:Namespace = loadNamespace()
    private var userEnvironment:Map[String,String] = Map.empty
    private var userConfig:Map[String,String] = Map.empty

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

    protected def loadUserSettings() : Unit = {
        def getKeyValuesArray(root:JsonNode, path:String) : Map[String,String] = {
            val env = root.path(path)
            if (env.isArray) {
                val array = env.asInstanceOf[ArrayNode]
                val entries = array.elements().asScala.map(_.asText()).toSeq
                splitSettings(entries).toMap
            }
            else {
                Map.empty
            }
        }
        val settingsFile = new File(".flowman-env.yml")
        if (settingsFile.exists()) {
            logger.info(s"Loading user overrides from ${settingsFile.getAbsoluteFile.getCanonicalPath}")
            try {
                val settings = ObjectMapper.read[JsonNode](settingsFile)
                userEnvironment = getKeyValuesArray(settings, "environment")
                userConfig = getKeyValuesArray(settings, "config")
            }
            catch {
                case NonFatal(ex) =>
                    logger.warn(s"Cannot load user settings '.flowman-env.yml':\n  ${reasons(ex)}")
            }
        }
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
                userConfig ++
                additionalConfigs

        val pluginJars = pluginManager.jars
            // Deduplicate file names to avoid problems when creating a Spark session
            .groupBy(_.getName)
            .map(_._2.head)
            .toSeq

        // Create Flowman Session, which also includes a Spark Session
        val builder = Session.builder()
            .withNamespace(namespace)
            .withConfig(allConfigs)
            .withEnvironment(userEnvironment)
            .withEnvironment(additionalEnvironment)
            .withProfiles(profiles)
            .withJars(pluginJars.map(_.toString))

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
