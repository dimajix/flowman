package com.dimajix.flowman.execution

import java.io.StringWriter
import java.time.Duration
import java.time.LocalDateTime
import java.time.Period

import scala.collection.mutable

import org.apache.velocity.VelocityContext
import org.apache.velocity.app.VelocityEngine
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.Connection
import com.dimajix.flowman.spec.ConnectionIdentifier


object AbstractContext {
    private lazy val rootContext = {
        val context = new VelocityContext()
        context.put("Integer", classOf[java.lang.Integer])
        context.put("Float", classOf[java.lang.Float])
        context.put("LocalDateTime", classOf[LocalDateTime])
        context.put("Duration", classOf[Duration])
        context.put("Period", classOf[Period])
        context
    }
}

abstract class AbstractContext extends Context {
    private val logger = LoggerFactory.getLogger(classOf[RootContext])

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

    protected def updateFrom(context:AbstractContext) = {
        context._environment.foreach(kv => _environment.update(kv._1,kv._2))
        context._config.foreach(kv => _config.update(kv._1,kv._2))
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
    def config : Map[String,String] = _config.toMap.mapValues(_._1)
    /**
      * Returns the current environment used for replacing variables
      *
      * @return
      */
    def environment : Map[String,String] = _environment.toMap.mapValues(_._1)


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
            logger.warn(s"Ignoring changing final config variable $key=${currentValue._1} to $value")
        }
    }
    def unsetConfig(key:String, settingLevel: SettingLevel) : Unit = {
        val currentValue = _config.getOrElse(key, ("", SettingLevel.NONE.level))
        if (currentValue._2 <= settingLevel.level) {
            _config.remove(key)
        }
        else {
            logger.warn(s"Ignoring removing final config variable $key=${currentValue._1}")
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
            logger.warn(s"Ignoring changing final environment variable $key=${currentValue._1} to $value")
        }
    }
    def unsetEnvironment(key:String, settingLevel: SettingLevel) : Unit = {
        val currentValue = _environment.getOrElse(key, ("", SettingLevel.NONE.level))
        if (currentValue._2 <= settingLevel.level) {
            _environment.remove(key)
            templateContext.remove(key)
        }
        else {
            logger.warn(s"Ignoring removing final config variable $key=${currentValue._1}")
        }
    }

    def setDatabases(databases:Map[String,Connection], settingLevel: SettingLevel) : Unit = {
        databases.foreach(kv => setDatabase(kv._1, kv._2, settingLevel))
    }
    def setDatabase(name:String, database:Connection, settingLevel: SettingLevel) : Unit = {
        val currentValue = _databases.getOrElse(name, (null, SettingLevel.NONE.level))
        if (currentValue._2 <= settingLevel.level) {
            _databases.update(name, (database, settingLevel.level))
        }
        else {
            logger.warn(s"Ignoring changing final database $name")
        }
    }
}
