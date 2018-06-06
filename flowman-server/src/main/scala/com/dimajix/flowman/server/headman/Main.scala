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

package com.dimajix.flowman.server.headman

import java.io.File

import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory

import com.dimajix.flowman.namespace.Namespace
import com.dimajix.flowman.plugin.PluginManager


object Main {
    def main(args: Array[String]) : Unit = {
        val options = new Arguments(args)
        val server = new Main(options)

        val result = server.run()
        System.exit(if (result) 0 else 1)
    }
}


class Main(args:Arguments) {
    private val logger = LoggerFactory.getLogger(classOf[Main])

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

    def run(): Boolean = {
        val log4j = System.getProperty("log4j.configuration")
        if (log4j == null || log4j.isEmpty) {
            val loader = Thread.currentThread.getContextClassLoader
            val url = loader.getResource("com/dimajix/flowman/log4j-defaults.properties")
            PropertyConfigurator.configure(url)
            logger.debug(s"Loaded Logging configuration from $url")
        }

        true
    }
}
