/*
 * Copyright (C) 2022 The Flowman Authors
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

package com.dimajix.spark.testing

import org.apache.log4j.PropertyConfigurator
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.slf4j.LoggerFactory


trait Logging extends BeforeAndAfterAll {
    this: Suite =>
    override def beforeAll(): Unit = {
        super.beforeAll()

        setupLogging()
    }

    protected def setupLogging(): Unit = {
        val loader = Thread.currentThread.getContextClassLoader
        if (isLog4j2()) {
            val log4j = System.getProperty("log4j.configurationFile")
            val log4j2 = System.getProperty("log4j2.configurationFile")
            if ((log4j == null || log4j.isEmpty) && (log4j2 == null || log4j2.isEmpty)) {
                val defaultLogProps = "com/dimajix/spark/testing/log4j2-defaults.properties"
                val url = loader.getResource(defaultLogProps)
                val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
                context.setConfigLocation(url.toURI)
            }
        }
        else {
            val log4j = System.getProperty("log4j.configuration")
            if (log4j == null || log4j.isEmpty) {
                val configUrl = loader.getResource("com/dimajix/spark/testing/log4j-defaults.properties")
                PropertyConfigurator.configure(configUrl)
            }
        }
    }

    private def isLog4j2(): Boolean = {
        // This distinguishes the log4j 1.2 binding, currently
        // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
        // org.apache.logging.slf4j.Log4jLoggerFactory
        val binderClass: String = LoggerFactory.getILoggerFactory.getClass.getName
        //val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
        "org.apache.logging.slf4j.Log4jLoggerFactory".equals(binderClass)
    }
}
