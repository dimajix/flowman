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

import scala.collection.mutable

import org.apache.velocity.VelocityContext
import org.slf4j.Logger

import com.dimajix.flowman.spec.Connection
import com.dimajix.flowman.spec.Profile
import com.dimajix.flowman.util.Templating


object AbstractContext {
    private lazy val rootContext = Templating.newContext()

    abstract class Builder extends Context.Builder {
        private var _environment = Seq[(String,Any,SettingLevel)]()
        private var _config = Seq[(String,String,SettingLevel)]()
        private var _databases = Seq[(String, Connection, SettingLevel)]()

        override def build() : AbstractContext = {
            val context = createContext()
            _environment.foreach(v => context.setEnvironment(v._1, v._2, v._3))
            _config.foreach(v => context.setConfig(v._1, v._2, v._3))
            _databases.foreach(v => context.setDatabase(v._1, v._2, v._3))
            context
        }

        override def withConfig(config:Map[String,String], level:SettingLevel) : Builder = {
            _config = _config ++ config.map(kv => (kv._1, kv._2, level))
            this
        }
        override def withConnections(env:Map[String,Connection], level:SettingLevel) : Builder = {
            _databases = _databases ++ env.map(kv => (kv._1, kv._2, level))
            this
        }
        override def withEnvironment(env:Seq[(String,Any)], level:SettingLevel) : Builder = {
            _environment = _environment ++ env.map(kv => (kv._1, kv._2, level))
            this
        }
        protected def withProfile(profile:Profile, level:SettingLevel) : Builder = {
            withConfig(profile.config.toMap, level)
            withEnvironment(profile.environment, level)
            withConnections(profile.connections, level)
            this
        }

        protected def createContext() : AbstractContext
    }
}


abstract class AbstractContext extends Context {
    protected val logger:Logger

    private val _environment = mutable.Map[String,(Any, Int)]()
    private val _config = mutable.Map[String,(String, Int)]()
    private val _databases = mutable.Map[String, (Connection, Int)]()

    private lazy val templateEngine = Templating.newEngine()
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
    override def config : Map[String,String] = _config.mapValues(v => evaluate(v._1)).toMap
    override def rawConfig : Map[String,(String, Int)] = _config.toMap

    /**
      * Returns the current environment used for replacing variables
      *
      * @return
      */
    override def environment : Map[String,Any] = _environment.mapValues(_._1).toMap
    override def rawEnvironment : Map[String,(Any, Int)] = _environment.toMap


    private def setConfig(key:String, value:String, settingLevel: SettingLevel) : Unit = {
        val currentValue = _config.getOrElse(key, ("", SettingLevel.NONE.level))
        if (currentValue._2 <= settingLevel.level) {
            _config.update(key, (value, settingLevel.level))
        }
        else {
            logger.info(s"Ignoring changing final config variable $key=${currentValue._1} to '$value'")
        }
    }
    private def unsetConfig(key:String, settingLevel: SettingLevel) : Unit = {
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
      */
    private def setEnvironment(key:String, value:Any, settingLevel: SettingLevel) : Unit = {
        val currentValue = _environment.getOrElse(key, ("", SettingLevel.NONE.level))
        if (currentValue._2 <= settingLevel.level) {
            val finalValue = value match {
                case s:String => evaluate(s)
                case v:Any => v
            }
            _environment.update(key, (finalValue, settingLevel.level))
            templateContext.put(key, finalValue)
        }
        else {
            logger.info(s"Ignoring changing final environment variable $key=${currentValue._1} to '$value'")
        }
    }
    private def unsetEnvironment(key:String, settingLevel: SettingLevel) : Unit = {
        val currentValue = _environment.getOrElse(key, ("", SettingLevel.NONE.level))
        if (currentValue._2 <= settingLevel.level) {
            _environment.remove(key)
            templateContext.remove(key)
        }
        else {
            logger.info(s"Ignoring removing final config variable $key=${currentValue._1}")
        }
    }

    private def setDatabase(name:String, database:Connection, settingLevel: SettingLevel) : Unit = {
        val currentValue = _databases.getOrElse(name, (null, SettingLevel.NONE.level))
        if (currentValue._2 <= settingLevel.level) {
            _databases.update(name, (database, settingLevel.level))
        }
        else {
            logger.info(s"Ignoring changing final database $name")
        }
    }
}
