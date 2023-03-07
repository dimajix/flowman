/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.plugin

import java.io.File
import java.net.URL
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spi.PluginListener


object PluginManager {
    class Builder {
        private var _pluginDir:File = new File("")

        def withPluginDir(dir: File): Builder = {
            _pluginDir = dir
            this
        }

        def build() : PluginManager = {
            new PluginManager(_pluginDir)
        }
    }

    def builder() : Builder = new Builder
}

/**
  * Helper class for loading Plugins
  */
final class PluginManager private (_pluginDir:File) {
    private val logger = LoggerFactory.getLogger(classOf[PluginManager])
    private val _plugins:mutable.Map[String,Plugin] = mutable.Map()
    private val classLoader = {
        // Replaced class loader
        val curLoader = Thread.currentThread().getContextClassLoader()
        try {
            curLoader.asInstanceOf[URLClassLoader]
        } catch {
            case _:ClassCastException =>
                val loader = new URLClassLoader(Array(), curLoader)
                Thread.currentThread().setContextClassLoader(loader)
                loader
        }
    }

    /**
      * Loads a named plugin. The plugin will be searched in the plugin directory
      * @param name
      */
    def load(name:String) : Unit = {
        val plugin = Plugin.read.file(new File(_pluginDir, name))
        load(plugin)
    }

    /**
      * Loads the runtime components of the given plugin. This will add all JARs to the current classpath and optionally
      * also add them to the SparkSession
      * @param plugin
      */
    def load(plugin:Plugin) : Unit = {
        logger.info(s"Loading Plugin ${plugin.name}")
        if (_plugins.contains(plugin.name))
            throw new UnsupportedOperationException(s"Plugin with name ${plugin.name} is already loaded")
        _plugins.update(plugin.name, plugin)

        // Resolve all JAR files from the plugin
        val jarFiles = pluginJars(plugin).map(_.toUri.toURL)

        // Extend classpath
        //val classLoader =  Thread.currentThread().getContextClassLoader().asInstanceOf[URLClassLoader]
        try {
            val method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
            method.setAccessible(true)
            jarFiles.foreach(jar => method.invoke(classLoader, jar))
        } catch {
            case t: Throwable => logger.error(s"Cannot add plugin ${plugin.name} to classpath", t)
        }

        // Inform all interested parties that a Plugin has been loaded
        PluginListener.listeners
            .foreach(_.pluginLoaded(plugin, classLoader))
    }

    /**
      * Returns the directory where all plugins are located and loaded from
      * @return
      */
    def pluginDir : File = _pluginDir

    /**
      * Returns a Map of all currently loaded plugins
      * @return
      */
    def plugins : Map[String,Plugin] = _plugins.toMap

    /**
      * Returns all JARs from all loaded plugins
      * @return
      */
    def jars : Seq[File] = {
        _plugins.toSeq
            .flatMap { case (_,p) => pluginJars(p) }
            .map(_.toFile)
    }

    private def pluginJars(plugin:Plugin) : Seq[Path] = {
        plugin.jars.flatMap { file =>
            val dir = file.getAbsoluteFile.toPath.getParent
            val matcher = dir.getFileSystem.getPathMatcher("glob:" + file.getName)
            Files.list(dir)
                .iterator().asScala
                .filter(path => matcher.matches(path.getFileName))
        }.distinct
    }
}
