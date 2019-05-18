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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.velocity.VelocityContext
import org.slf4j.Logger

import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.spec.Profile
import com.dimajix.flowman.spec.connection.Connection
import com.dimajix.flowman.spec.connection.ConnectionSpec
import com.dimajix.flowman.templating.Velocity


object AbstractContext {
    private lazy val rootContext = Velocity.newContext()

    abstract class Builder(parent:Context, defaultSettingLevel:SettingLevel) extends Context.Builder {
        private var _environment = Seq[(String,Any,SettingLevel)]()
        private var _config = Seq[(String,String,SettingLevel)]()
        private var _connections = Seq[(String, ConnectionSpec, SettingLevel)]()

        protected val logger:Logger

        /**
          * Builds a new Context with the configuration as done before in the Builder
          * @return
          */
        override def build() : AbstractContext = {
            val rawEnvironment = mutable.Map[String,(Any, Int)]()
            val rawConfig = mutable.Map[String,(String, Int)]()
            val rawConnections = mutable.Map[String, (ConnectionSpec, Int)]()

            if (parent != null) {
                parent.rawEnvironment.foreach(kv => rawEnvironment.update(kv._1, kv._2))
                parent.rawConfig.foreach(kv => rawConfig.update(kv._1, kv._2))
            }

            val templateEngine = Velocity.newEngine()
            val templateContext = new VelocityContext(AbstractContext.rootContext)

            def evaluate(string:String) : String = {
                if (string != null) {
                    val output = new StringWriter()
                    templateEngine.evaluate(templateContext, output, "context", string)
                    output.getBuffer.toString
                }
                else {
                    null
                }
            }

            def addConfig(key:String, value:String, settingLevel: SettingLevel) : Unit = {
                val currentValue = rawConfig.getOrElse(key, ("", SettingLevel.NONE.level))
                if (currentValue._2 <= settingLevel.level) {
                    rawConfig.update(key, (value, settingLevel.level))
                }
                else {
                    logger.info(s"Ignoring changing final config variable $key=${currentValue._1} to '$value'")
                }
            }

            def addEnvironment(key:String, value:Any, settingLevel: SettingLevel) : Unit = {
                val currentValue = rawEnvironment.getOrElse(key, ("", SettingLevel.NONE.level))
                if (currentValue._2 <= settingLevel.level) {
                    val finalValue = value match {
                        case s:String => evaluate(s)
                        case v:Any => v
                    }
                    rawEnvironment.update(key, (finalValue, settingLevel.level))
                    templateContext.put(key, finalValue)
                }
                else {
                    logger.info(s"Ignoring changing final environment variable $key=${currentValue._1} to '$value'")
                }
            }

            def addConnection(name:String, connection:ConnectionSpec, settingLevel: SettingLevel) : Unit = {
                val currentValue = rawConnections.getOrElse(name, (null, SettingLevel.NONE.level))
                if (currentValue._2 <= settingLevel.level) {
                    rawConnections.update(name, (connection, settingLevel.level))
                }
                else {
                    logger.info(s"Ignoring changing final database $name")
                }
            }

            _environment.foreach(v => addEnvironment(v._1, v._2, v._3))
            _config.foreach(v => addConfig(v._1, v._2, v._3))
            _connections.foreach(v => addConnection(v._1, v._2, v._3))
            val context = createContext(rawEnvironment.toMap, rawConfig.toMap, rawConnections.mapValues(_._1).toMap)
            context
        }

        /**
          * Set Spark configuration options
          * @param config
          * @return
          */
        override def withConfig(config:Map[String,String]) : Builder = {
            require(config != null)
            withConfig(config, defaultSettingLevel)
            this
        }
        /**
          * Set Spark configuration options
          * @param config
          * @return
          */
        override def withConfig(config:Map[String,String], level:SettingLevel) : Builder = {
            require(config != null)
            require(level != null)
            _config = _config ++ config.map(kv => (kv._1, kv._2, level))
            this
        }

        /**
          * Add some connections
          * @param connections
          * @return
          */
        override def withConnections(connections:Map[String,ConnectionSpec]) : Builder = {
            require(connections != null)
            withConnections(connections, defaultSettingLevel)
            this
        }
        /**
          * Add some connections
          * @param connections
          * @return
          */
        override def withConnections(connections:Map[String,ConnectionSpec], level:SettingLevel) : Builder = {
            require(connections != null)
            require(level != null)
            _connections = _connections ++ connections.map(kv => (kv._1, kv._2, level))
            this
        }

        /**
          * Set environment variables. All variables will be interpolated using the previously defined
          * variables
          * @param env
          * @return
          */
        override def withEnvironment(env: Seq[(String, Any)]): Builder = {
            require(env != null)
            withEnvironment(env, defaultSettingLevel)
            this
        }
        /**
          * Set environment variables. All variables will be interpolated using the previously defined
          * variables
          * @param env
          * @return
          */
        override def withEnvironment(env:Seq[(String,Any)], level:SettingLevel) : Builder = {
            require(env != null)
            require(level != null)
            _environment = _environment ++ env.map(kv => (kv._1, kv._2, level))
            this
        }

        /**
          * Activate some profile
          * @param profile
          * @return
          */
        override def withProfile(profile:Profile) : Builder = {
            require(profile != null)
            withProfile(profile, defaultSettingLevel)
            this
        }
        /**
          * Activate some profile using a specific setting priority
          * @param profile
          * @return
          */
        protected def withProfile(profile:Profile, level:SettingLevel) : Builder = {
            require(profile != null)
            require(level != null)
            withConfig(profile.config.toMap, level)
            withEnvironment(profile.environment, level)
            withConnections(profile.connections, level)
            this
        }

        protected def createContext(env:Map[String,(Any, Int)], config:Map[String,(String, Int)], connections:Map[String, ConnectionSpec]) : AbstractContext
    }
}


abstract class AbstractContext(
          override val rawEnvironment:Map[String,(Any, Int)],
          override val rawConfig:Map[String,(String, Int)]
) extends Context {
    private lazy val templateEngine = Velocity.newEngine()
    protected lazy val templateContext = {
        val context = new VelocityContext(AbstractContext.rootContext)
        environment.foreach(kv => context.put(kv._1, kv._2))
        context
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
    override def config : Map[String,String] = rawConfig.mapValues(v => evaluate(v._1))

    /**
      * Returns the current environment used for replacing variables
      *
      * @return
      */
    override def environment : Map[String,Any] = rawEnvironment.mapValues(_._1)

    /**
      * Returns the FileSystem as configured in Hadoop
      * @return
      */
    override def fs: FileSystem = root.fs

    /**
      * Returns a SparkConf object, which contains all Spark settings as specified in the conifguration. The object
      * is not necessarily the one used by the Spark Session!
      * @return
      */
    override def sparkConf : SparkConf = root.sparkConf

    /**
      * Returns a Hadoop Configuration object which contains all settings form the configuration. The object is not
      * necessarily the one used by the active Spark session
      * @return
      */
    override def hadoopConf : Configuration = root.hadoopConf
}
