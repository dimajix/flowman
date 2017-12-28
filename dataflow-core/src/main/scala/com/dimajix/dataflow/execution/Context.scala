package com.dimajix.dataflow.execution

import java.io.StringWriter
import java.time.Duration
import java.time.LocalDateTime
import java.time.Period

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.velocity.VelocityContext
import org.apache.velocity.app.VelocityEngine
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.spec.Database
import com.dimajix.dataflow.spec.Profile
import com.dimajix.dataflow.spec.model.Relation
import com.dimajix.dataflow.util.splitSettings


object Context {
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


class Context(val rootSession:SparkSession) {
    private val logger = LoggerFactory.getLogger(classOf[Executor])

    private lazy val templateEngine = {
        val ve = new VelocityEngine()
        ve.init()
        ve
    }
    private lazy val templateContext = {
        val context = new VelocityContext(Context.rootContext)
        _environment.foreach(kv => context.put(kv._1, kv._2))
        context
    }
    private val _tables = mutable.Map[String, DataFrame]()
    private val _environment: mutable.Map[String,(String, Boolean)] = mutable.Map()
    private val _config: mutable.Map[String,(String, Boolean)] = mutable.Map()
    private val _databases: mutable.Map[String, Database] = mutable.Map()
    private val _relations: mutable.Map[String, Relation] = mutable.Map()
    private var _session: Option[SparkSession] = None

    /**
      * Creates a derived context. The derived context will contain a copy of all environment variables etc.
      * Changes to the parent context will not influence the derived context, it is a completely separate instance
      * and will only share very basic settings
      *
      * @param context
      */
    private def this(context:Context) = {
        this(context.rootSession)
        _environment.foreach(kv => context._environment.update(kv._1, kv._2))
        _config.foreach(kv => context._config.update(kv._1, kv._2))
        _databases.foreach(kv => context._databases.update(kv._1, kv._2))
        _relations.foreach(kv => context._relations.update(kv._1, kv._2))
    }

    def hasSession : Boolean = _session.isDefined

    /**
      * Returns (or lazily creates) a SparkSession of this context. The SparkSession will be derived from the global
      * SparkSession, but a new derived session with a separate namespace will be created.
      *
      * @return
      */
    def session : SparkSession = {
        logger.info("Creating new local session for context")
        val session = rootSession.newSession()
        _session = Some(session)
        _config.foreach(kv => session.conf.set(kv._1, kv._2._1))
        session
    }

    /**
      * Evaluates a string containing expressions to be processed.
      *
      * @param string
      * @return
      */
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

    def databases : Map[String,Database] = _databases.toMap
    def getDatabase(name:String) : Database = {
        _databases.getOrElse(name, throw new NoSuchElementException(s"Database $name not found"))
    }

    def relations : Map[String,Relation] = _relations.toMap
    def getRelation(name:String) : Relation = {
        _relations.getOrElse(name, throw new NoSuchElementException(s"Relation $name not found"))
    }

    def tables : Map[String,DataFrame] = _tables.toMap
    /**
      * Register a temporary table
      * @param name
      * @param df
      */
    def putTable(name:String, df:DataFrame) : Unit= {
        _tables.update(name, df)
        df.createOrReplaceTempView(name)
    }
    def removeTable(name:String) : Unit = {
        _tables.remove(name)
    }
    def getTable(name:String) : DataFrame = {
        _tables(name)
    }
    def getTable(name:String, op: => DataFrame) : DataFrame = {
        _tables.getOrElseUpdate(name, op)
    }

    /**
      * Returns all configuration options as a key-value map
      * @return
      */
    def config : Map[String,String] = _config.toMap.mapValues(_._1)
    def setConfig(config:Map[String,String], asFinal:Boolean) : Unit = {
        setConfig(config.toSeq, asFinal)
    }
    def setConfig(config:Seq[(String,String)], asFinal:Boolean) : Unit = {
        config.foreach(kv => setConfig(kv._1, kv._2, asFinal))
    }
    def setConfig(key:String, value:String, asFinal:Boolean=false) : Unit = {
        val currentValue = _config.getOrElse(key, ("", false))
        if (!currentValue._2) {
            val finalValue = evaluate(value)
            _config.update(key, (finalValue, asFinal))
            _session.foreach(session => session.conf.set(key, finalValue))
        }
        else {
            logger.warn(s"Ignoring changing final config variable $key=${currentValue._1} to $value")
        }
    }
    def unsetConfig(key:String) : Unit = {
        val currentValue = _config.getOrElse(key, ("", false))
        if (!currentValue._2) {
            _config.remove(key)
            _session.foreach(session => session.conf.unset(key))
        }
        else {
            logger.warn(s"Ignoring removing final config variable $key=${currentValue._1}")
        }
    }

    /**
      * Returns the current environment used for replacing variables
      *
      * @return
      */
    def environment : Map[String,String] = _environment.toMap.mapValues(_._1)
    /**
      * Updates environment variables
      *
      * @param env
      */
    def setEnvironment(env:Map[String,String], asFinal:Boolean) : Unit = {
        setEnvironment(env.toSeq, asFinal)
    }
    def setEnvironment(env:Seq[(String,String)], asFinal:Boolean) : Unit = {
        env.foreach(kv => setEnvironment(kv._1, kv._2, asFinal))
    }
    def setEnvironment(key:String, value:String, asFinal:Boolean=false) : Unit = {
        val currentValue = _environment.getOrElse(key, ("", false))
        if (!currentValue._2) {
            val finalValue = evaluate(value)
            _environment.update(key, (finalValue, asFinal))
            templateContext.put(key, finalValue)
        }
        else {
            logger.warn(s"Ignoring changing final environment variable $key=${currentValue._1} to $value")
        }
    }
    def unsetEnvironment(key:String) : Unit = {
        val currentValue = _environment.getOrElse(key, ("", false))
        if (!currentValue._2) {
            _environment.remove(key)
            templateContext.remove(key)
        }
        else {
            logger.warn(s"Ignoring removing final config variable $key=${currentValue._1}")
        }
    }

    /**
      * Creates a new chained context with additional environment variables
      * @param env
      * @return
      */
    def withEnvironment(env:Map[String,String]) : Context = {
        withEnvironment(env.toSeq)
    }
    def withEnvironment(env:Seq[(String,String)]) : Context = {
        val context = new Context(this)
        context.setEnvironment(env, false)
        context
    }

    /**
      * Creates a new chained context with additional Spark configuration variables
      * @param env
      * @return
      */
    def withConfig(env:Map[String,String]) : Context = {
        withConfig(env.toSeq)
    }
    def withConfig(env:Seq[(String,String)]) : Context = {
        val context = new Context(this)
        context.setConfig(env, true)
        context
    }

    /**
      * Creates a new chained context with additional properties from a profile
      * @param profile
      * @return
      */
    def withProfile(profile:Profile) : Context = {
        implicit val context = new Context(this)
        context.setConfig(profile.config, true)
        context.setEnvironment(profile.environment, true)
        profile.databases.foreach(kv => context._databases.update(kv._1, kv._2))
        context
    }

    /**
      * Cleans up the session
      */
    def cleanup() : Unit = {
        // Unregister all temporary tables
        _session.foreach(session => {
            logger.info("Cleaning up temporary tables and caches")
            val catalog = session.catalog
            catalog.clearCache()
            _tables.foreach(kv => catalog.dropTempView(kv._1))
            _tables.foreach(kv => kv._2.unpersist())
        })
    }
}
