/*
 * Copyright 2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.common

import java.io.File
import java.io.IOException
import java.net.MalformedURLException
import java.net.URL
import java.util.Locale
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory


class Logging
object Logging {
    private lazy val logger = LoggerFactory.getLogger(classOf[Logging])
    private var props:Properties = null
    private var level:String = null

    def init() : Unit = {
        val loader = Thread.currentThread.getContextClassLoader
        val log4j = System.getProperty("log4j.configuration")
        val url =
            if (log4j == null || log4j.isEmpty) {
                loader.getResource("com/dimajix/flowman/log4j-defaults.properties")
            } else {
                try {
                    new URL(log4j)
                }
                catch {
                    case _:MalformedURLException => new File(log4j).toURI.toURL
                }
            }
        props = loadProperties(url)
        reconfigureLogging()
        logger.debug(s"Loaded logging configuration from $url")
    }

    private def loadProperties(url:URL) : Properties = {
        try {
            val urlConnection = url.openConnection
            urlConnection.setUseCaches(false)
            val inputStream = urlConnection.getInputStream
            try {
                val loaded = new Properties
                loaded.load(inputStream)
                loaded
            } finally {
                inputStream.close()
            }
        } catch {
            case e: IOException =>
                logger.warn("Could not read configuration file from URL [" + url + "].", e)
                logger.warn("Ignoring configuration file [" + url + "].")
                null
        }
    }

    def setSparkLogging(logLevel:String) : Unit = {
        // Adjust Spark logging level
        logger.debug(s"Setting Spark log level to ${logLevel}")

        val upperCased = logLevel.toUpperCase(Locale.ENGLISH)
        val l = org.apache.log4j.Level.toLevel(upperCased)
        org.apache.log4j.Logger.getLogger("org").setLevel(l)
        org.apache.log4j.Logger.getLogger("akka").setLevel(l)
        org.apache.log4j.Logger.getLogger("hive").setLevel(l)
    }

    def setLogging(logLevel:String) : Unit = {
        logger.debug(s"Setting global log level to ${logLevel}")
        level = logLevel.toUpperCase(Locale.ENGLISH)

        reconfigureLogging()
    }

    private def reconfigureLogging() : Unit = {
        // Create new logging properties with all levels being replaced
        val newProps = new Properties()
        if (props != null) {
            props.asScala.foreach { case (k, v) => newProps.setProperty(k, v) }
            if (level != null) {
                newProps.keys().asScala.collect { case s: String => s }
                    .toList
                    .filter(k => k.startsWith("log4j.logger."))
                    .foreach(k => newProps.setProperty(k, level))
            }
            PropertyConfigurator.configure(newProps)
        }

        if (level != null) {
            val l = org.apache.log4j.Level.toLevel(level)
            org.apache.log4j.Logger.getRootLogger().setLevel(l)
        }
    }
}
