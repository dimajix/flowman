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

package com.dimajix.flowman.execution

import java.io.StringWriter
import java.time.Duration
import java.time.LocalDateTime
import java.time.Period

import scala.collection.mutable

import org.apache.velocity.VelocityContext
import org.apache.velocity.app.VelocityEngine
import org.slf4j.Logger

import com.dimajix.flowman.spec.Connection


object AbstractContext {
    private object SystemWrapper {
        def getenv(name:String) : String = System.getenv(name)
    }

    private lazy val rootContext = {
        val context = new VelocityContext()
        context.put("Integer", classOf[java.lang.Integer])
        context.put("Float", classOf[java.lang.Float])
        context.put("LocalDateTime", classOf[LocalDateTime])
        context.put("Duration", classOf[Duration])
        context.put("Period", classOf[Period])
        context.put("System", SystemWrapper)
        context
    }
}

abstract class AbstractContext extends Context {
    protected val logger:Logger

    private val _environment = mutable.Map[String,(String, Int)]()
    private val _config = mutable.Map[String,(String, Int)]()
    private val _databases = mutable.Map[String, (Connection, Int)]()

    private lazy val templateEngine = {
        val ve = new VelocityEngine()
        ve.init()
        ve
    }
    protected lazy val templateContext = {
        val context = new VelocityContext(AbstractContext.rootContext)
        environment.foreach(kv => context.put(kv._1, kv._2))
        context
    }

    protected def databases : Map[String,Connection] = _databases.mapValues(_._1).toMap

    protected def updateFrom(context:Context) = {
        context.rawEnvironment.foreach(kv => _environment.update(kv._1,kv._2))
        context.rawConfig.foreach(kv => _config.update(kv._1,kv._2))
    }

    /**
      * Evaluates a string containing expressions to be processed.
      *
      * @param string
      * @return
      */
    override def evaluate(string:String) : String = {
        if (string != null) {
            val output = new StringWriter()
            templateEngine.evaluate(templateContext, output, "context", string)
            output.getBuffer.toString
        }
        else {
            null
        }
    }

    /**
      * Returns all configuration options as a key-value map
      * @return
      */
    override def config : Map[String,String] = _config.toMap.mapValues(_._1)
    /**
      * Returns the current environment used for replacing variables
      *
      * @return
      */
    override def environment : Map[String,String] = _environment.toMap.mapValues(_._1)

    override def rawEnvironment : Map[String,(String, Int)] = _environment.toMap
    override def rawConfig : Map[String,(String, Int)] = _config.toMap


    def setConfig(config:Map[String,String], settingLevel: SettingLevel) : Unit = {
        setConfig(config.toSeq, settingLevel)
    }
    def setConfig(config:Seq[(String,String)], settingLevel: SettingLevel) : Unit = {
        config.foreach(kv => setConfig(kv._1, kv._2, settingLevel))
    }
    def setConfig(key:String, value:String, settingLevel: SettingLevel) : Unit = {
        val currentValue = _config.getOrElse(key, ("", SettingLevel.NONE.level))
        if (currentValue._2 <= settingLevel.level) {
            val finalValue = evaluate(value)
            _config.update(key, (finalValue, settingLevel.level))
        }
        else {
            logger.info(s"Ignoring changing final config variable $key=${currentValue._1} to '$value'")
        }
    }
    def unsetConfig(key:String, settingLevel: SettingLevel) : Unit = {
        val currentValue = _config.getOrElse(key, ("", SettingLevel.NONE.level))
        if (currentValue._2 <= settingLevel.level) {
            _config.remove(key)
        }
        else {
            logger.info(s"Ignoring removing final config variable $key=${currentValue._1}")
        }
    }

    /**
      * Updates environment variables
      *
      * @param env
      */
    def setEnvironment(env:Map[String,String], settingLevel: SettingLevel) : Unit = {
        setEnvironment(env.toSeq, settingLevel)
    }
    def setEnvironment(env:Seq[(String,String)], settingLevel: SettingLevel) : Unit = {
        env.foreach(kv => setEnvironment(kv._1, kv._2, settingLevel))
    }
    def setEnvironment(key:String, value:String, settingLevel: SettingLevel) : Unit = {
        val currentValue = _environment.getOrElse(key, ("", SettingLevel.NONE.level))
        if (currentValue._2 <= settingLevel.level) {
            val finalValue = evaluate(value)
            _environment.update(key, (finalValue, settingLevel.level))
            templateContext.put(key, finalValue)
        }
        else {
            logger.info(s"Ignoring changing final environment variable $key=${currentValue._1} to '$value'")
        }
    }
    def unsetEnvironment(key:String, settingLevel: SettingLevel) : Unit = {
        val currentValue = _environment.getOrElse(key, ("", SettingLevel.NONE.level))
        if (currentValue._2 <= settingLevel.level) {
            _environment.remove(key)
            templateContext.remove(key)
        }
        else {
            logger.info(s"Ignoring removing final config variable $key=${currentValue._1}")
        }
    }

    def setConnections(databases:Map[String,Connection], settingLevel: SettingLevel) : Unit = {
        databases.foreach(kv => setDatabase(kv._1, kv._2, settingLevel))
    }
    def setDatabase(name:String, database:Connection, settingLevel: SettingLevel) : Unit = {
        val currentValue = _databases.getOrElse(name, (null, SettingLevel.NONE.level))
        if (currentValue._2 <= settingLevel.level) {
            _databases.update(name, (database, settingLevel.level))
        }
        else {
            logger.info(s"Ignoring changing final database $name")
        }
    }
}
