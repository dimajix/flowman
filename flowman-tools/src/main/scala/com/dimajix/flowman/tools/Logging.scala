package com.dimajix.flowman.tools

import java.util.Locale

import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory


class Logging
object Logging {
    private lazy val logger = LoggerFactory.getLogger(classOf[Logging])

    def setup(sparkLogging:Option[String] = None) : Unit = {
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
