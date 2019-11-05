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
import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.velocity.VelocityContext
import org.slf4j.Logger

import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.spec.Profile
import com.dimajix.flowman.spec.connection.ConnectionSpec
import com.dimajix.flowman.templating.RecursiveValue
import com.dimajix.flowman.templating.Velocity


object AbstractContext {
    private lazy val rootContext = Velocity.newContext()

    abstract class Builder[B <: Builder[B,C], C <: Context](parent:Context, defaultSettingLevel:SettingLevel) /*extends Context.Builder[B,C]*/ { this:B =>
        private var _environment = Seq[(String,Any,SettingLevel)]()
        private var _config = Seq[(String,String,SettingLevel)]()
        private var _connections = Seq[(String, ConnectionSpec, SettingLevel)]()

        protected val logger:Logger

        /**
          * Builds a new Context with the configuration as done before in the Builder
          * @return
          */
        def build() : C = {
            val rawEnvironment = mutable.Map[String,(Any, Int)]()
            val rawConfig = mutable.Map[String,(String, Int)]()
            val rawConnections = mutable.Map[String, (ConnectionSpec, Int)]()

            if (parent != null) {
                parent.rawEnvironment.foreach(kv => rawEnvironment.update(kv._1, kv._2))
                parent.rawConfig.foreach(kv => rawConfig.update(kv._1, kv._2))
            }

            def addConfig(key:String, value:String, settingLevel: SettingLevel) : Unit = {
                val currentValue = rawConfig.getOrElse(key, ("", SettingLevel.NONE.level))
                if (currentValue._2 <= settingLevel.level) {
                    rawConfig.update(key, (value, settingLevel.level))
                }
                else {
                    logger.info(s"Ignoring changing final config variable '$key=${currentValue._1}' to '$value'")
                }
            }

            def addEnvironment(key:String, value:Any, settingLevel: SettingLevel) : Unit = {
                val currentValue = rawEnvironment.getOrElse(key, ("", SettingLevel.NONE.level))
                if (currentValue._2 <= settingLevel.level) {
                    rawEnvironment.update(key, (value, settingLevel.level))
                }
                else {
                    logger.info(s"Ignoring changing final environment variable '$key=${currentValue._1}' to '$value'")
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

            createContext(rawEnvironment.toMap, rawConfig.toMap, rawConnections.mapValues(_._1).toMap)
        }

        /**
          * Set Spark configuration options
          * @param config
          * @return
          */
        def withConfig(config:Map[String,String]) : B = {
            require(config != null)
            withConfig(config, defaultSettingLevel)
            this
        }

        /**
          * Set Spark configuration options
          * @param config
          * @return
          */
        def withConfig(config:Map[String,String], level:SettingLevel) : B = {
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
        def withConnections(connections:Map[String,ConnectionSpec]) : B = {
            require(connections != null)
            withConnections(connections, defaultSettingLevel)
            this
        }
        /**
          * Add some connections
          * @param connections
          * @return
          */
        def withConnections(connections:Map[String,ConnectionSpec], level:SettingLevel) : B = {
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
        def withEnvironment(env: Map[String, Any]): B = {
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
        def withEnvironment(env:Map[String,Any], level:SettingLevel) : B = {
            require(env != null)
            require(level != null)
            _environment = _environment ++ env.map(kv => (kv._1, kv._2, level))
            this
        }
        /**
         * Set environment variables. All variables will be interpolated using the previously defined
         * variables
         * @param key
         * @param value
         * @return
         */
        def withEnvironment(key: String, value:Any): B = {
            require(key != null)
            withEnvironment(key, value, defaultSettingLevel)
            this
        }
        /**
         * Set environment variables. All variables will be interpolated using the previously defined
         * variables
         * @param key
         * @param value
         * @return
         */
        def withEnvironment(key: String, value:Any, level:SettingLevel) : B = {
            require(key != null)
            require(level != null)
            _environment = _environment :+ ((key, value, level))
            this
        }

        /**
          * Activate some profile
          * @param profile
          * @return
          */
        def withProfile(profile:Profile) : B = {
            require(profile != null)
            withProfile(profile, defaultSettingLevel)
            this
        }
        /**
          * Activate some profile using a specific setting priority
          * @param profile
          * @return
          */
        def withProfile(profile:Profile, level:SettingLevel) : B = {
            require(profile != null)
            require(level != null)
            withConfig(profile.config, level)
            withEnvironment(profile.environment, level)
            withConnections(profile.connections, level)
            this
        }

        protected def createContext(env:Map[String,(Any, Int)], config:Map[String,(String, Int)], connections:Map[String, ConnectionSpec]) : C
    }
}


abstract class AbstractContext(
    parent:Context,
    override val rawEnvironment:Map[String,(Any, Int)],
    override val rawConfig:Map[String,(String, Int)]
) extends Context {
    protected final val templateEngine = Velocity.newEngine()
    protected final val templateContext = new VelocityContext(AbstractContext.rootContext)

    // Configure templating context
    rawEnvironment.foreach { case (key,(value,_)) =>
        val finalValue = value match {
            case s:String => RecursiveValue(templateEngine, templateContext, s)
            case v:Any => v
            case null => null
        }
        templateContext.put(key, finalValue)
    }

    private def evaluateNotNull(string:String, additionalValues:Map[String,AnyRef]) = {
        val output = new StringWriter()
        val context = if (additionalValues.nonEmpty)
            new VelocityContext(additionalValues, templateContext)
        else
            templateContext
        templateEngine.evaluate(context, output, "context", string)
        output.getBuffer.toString
    }

    /**
      * Evaluates a string containing expressions to be processed.
      *
      * @param string
      * @return
      */
    override def evaluate(string:String) : String = evaluate(string, Map())

    /**
      * Evaluates a string containing expressions to be processed. This variant also accepts a key-value Map
      * with additional values to be used for evaluation
      *
      * @param string
      * @return
      */
    override def evaluate(string:String, additionalValues:Map[String,AnyRef]) : String = {
        if (string != null)
            evaluateNotNull(string, additionalValues)
        else
            null
    }

    /**
      * Evaluates a string containing expressions to be processed.
      *
      * @param string
      * @return
      */
    def evaluate(string:Option[String]) : Option[String] = {
        string.map(evaluate).map(_.trim).filter(_.nonEmpty)
    }

    /**
      * Evaluates a string containing expressions to be processed. This variant also accepts a key-value Map
      * with additional values to be used for evaluation
      *
      * @param string
      * @return
      */
    def evaluate(string:Option[String], additionalValues:Map[String,AnyRef]) : Option[String] = {
        string.map(s => evaluate(s, additionalValues)).map(_.trim).filter(_.nonEmpty)
    }

    /**
      * Evaluates a key-value map containing values with expressions to be processed.
      *
      * @param map
      * @return
      */
    def evaluate(map: Map[String,String]): Map[String,String] = evaluate(map, Map())

    /**
      * Evaluates a key-value map containing values with expressions to be processed.  This variant also accepts a
      * key-value Map with additional values to be used for evaluation
      *
      * @param map
      * @return
      */
    override def evaluate(map: Map[String,String], additionalValues:Map[String,AnyRef]): Map[String,String] = {
        map.map { case(name,value) => (name, evaluate(value, additionalValues)) }
    }

    /**
      * Returns all configuration options as a key-value map
      * @return
      */
    override def config : Map[String,String] = rawConfig.map { case(k,v) => k -> evaluate(v._1) }

    /**
      * Returns the current environment used for replacing variables
      *
      * @return
      */
    override def environment : Map[String,Any] = rawEnvironment.map {
        case (k, (s: String, _)) => k -> evaluate(s)
        case (k, (any, _)) => k -> any
    }

    /**
      * Returns the FileSystem as configured in Hadoop
      * @return
      */
    override def fs: FileSystem = root.fs

    /**
     * Returns the FlowmanConf object, which contains all Flowman settings.
     * @return
     */
    override def flowmanConf : FlowmanConf = root.flowmanConf

    /**
      * Returns a SparkConf object, which contains all Spark settings as specified in the configuration. The object
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
