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

import java.io.File

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.dimajix.flowman.namespace.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spi.UdfProvider


class SessionBuilder {
    private var _sparkSession: SparkSession = _
    private var _sparkName = ""
    private var _sparkConfig = Map[String,String]()
    private var _environment = Map[String,String]()
    private var _profiles = Set[String]()
    private var _project:Project = _
    private var _namespace:Namespace = _
    private var _jars = Set[String]()

    /**
      * Injects an existing Spark session. If no session is provided, Flowman will create its own Spark session
      * @param session
      * @return
      */
    def withSparkSession(session:SparkSession) : SessionBuilder = {
        assert(session != null)
        _sparkSession = session
        this
    }
    def withSparkName(name:String) : SessionBuilder = {
        assert(name != null)
        _sparkName = name
        this
    }

    /**
      * Adds Spark config variables which actually will override any variables given in specs
      * @param config
      * @return
      */
    def withSparkConfig(config:Map[String,String]) : SessionBuilder = {
        assert(config != null)
        _sparkConfig = _sparkConfig ++ config
        this
    }

    /**
      * Adds environment variables which actually will override any variables given in specs
      * @param env
      * @return
      */
    def withEnvironment(env:Map[String,String]) : SessionBuilder = {
        assert(env != null)
        _environment = _environment ++ env
        this
    }

    /**
      * Adds a Namespace to source more configuration from
      * @param namespace
      * @return
      */
    def withNamespace(namespace:Namespace) : SessionBuilder = {
        _namespace = namespace
        this
    }

    /**
      * Adds a project to source more configurations from.
      * @param project
      * @return
      */
    def withProject(project:Project) : SessionBuilder = {
        _project = project
        this
    }

    /**
      * Adds a new profile to be activated
      * @param profile
      * @return
      */
    def withProfile(profile:String) : SessionBuilder = {
        _profiles = _profiles + profile
        this
    }

    /**
      * Adds a list of profile names to be activated. This does not remove any previously activated profile
      * @param profile
      * @return
      */
    def withProfiles(profile:Seq[String]) : SessionBuilder = {
        _profiles = _profiles ++ profile
        this
    }

    /**
      * Adds JAR files to this session which will be distributed to all Spark nodes
      * @param jars
      * @return
      */
    def withJars(jars:Seq[String]) : SessionBuilder = {
        _jars = _jars ++ jars
        this
    }

    /**
      * Build the Flowman session and applies all previously specified options
      * @return
      */
    def build() : Session = {
        val session = new Session(_namespace, _project, _sparkSession, _sparkName, _sparkConfig, _environment, _profiles, _jars)
        session
    }
}


object Session {
    def builder() = new SessionBuilder
}

/**
  * A Flowman session is used as the starting point for executing data flows. It contains information about the
  * Namespace, the Project and also managed a Spark session.
  * @param _namespace
  * @param _project
  * @param _sparkSession
  * @param _sparkName
  * @param _sparkConfig
  * @param _environment
  * @param _profiles
  * @param _jars
  */
class Session private[execution](
    _namespace:Namespace,
    _project:Project,
    _sparkSession:SparkSession,
    _sparkName:String,
    _sparkConfig:Map[String,String],
    _environment: Map[String,String],
    _profiles:Set[String],
    _jars:Set[String]
) {
    private val logger = LoggerFactory.getLogger(classOf[Session])

    private def sparkConfig :Map[String,String] = {
        if (_project != null) {
            logger.info("Using project specific Spark configuration settings")
            getContext(_project).config
        }
        else {
            logger.info("Using global Spark configuration settings")
            context.config
        }
    }

    private def sparkJars : Seq[String] = {
        _jars.toSeq
    }

    /**
      * Creates a new Spark Session for this DataFlow session
      *
      * @return
      */
    private def createOrReuseSession() : Option[SparkSession] = {
        if(_sparkSession != null) {
            logger.info("Reusing existing Spark session")
            val newSession = _sparkSession.newSession()
            _sparkConfig.foreach(kv => newSession.conf.set(kv._1,kv._2))
            Some(newSession)
        }
        else {
            logger.info("Creating new Spark session")
            Try {
                val sparkConf = new SparkConf()
                    .setAppName(_sparkName)
                    .setAll(sparkConfig.toSeq)
                _sparkConfig.foreach(kv => sparkConf.set(kv._1,kv._2))
                val master = System.getProperty("spark.master")
                if (master == null || master.isEmpty) {
                    logger.info("No Spark master specified - using local[*]")
                    sparkConf.setMaster("local[*]")
                    sparkConf.set("spark.sql.shuffle.partitions", "16")
                }
                SparkSession.builder()
                    .config(sparkConf)
                    .enableHiveSupport()
                    .getOrCreate()
            }
            match {
                case Success(session) =>
                    logger.info("Successfully created Spark Session")
                    Some(session)
                case Failure(e) =>
                    logger.error("Failed to create Spark Session.", e)
                    None
            }
        }

    }
    private def createSession() = {
        val sparkSession = createOrReuseSession()
        sparkSession.foreach(spark => {
            spark.conf.getAll.toSeq.sortBy(_._1).foreach { case (key, value)=> logger.info("Config: {} = {}", key: Any, value: Any) }
        })

        // Distribute additional Plugin jar files
        sparkSession.foreach(session => sparkJars.foreach(session.sparkContext.addJar))

        // Register special UDFs
        sparkSession.foreach(session => UdfProvider.providers.foreach(_.register(session.udf)))

        sparkSession
    }
    private var sparkSession:Option[SparkSession] = null

    private lazy val rootContext : RootContext = {
        val context = new RootContext(_namespace, _profiles.toSeq)
        context.setEnvironment(_environment, SettingLevel.GLOBAL_OVERRIDE)
        context.setConfig(_sparkConfig, SettingLevel.GLOBAL_OVERRIDE)
        if (_namespace != null) {
            _profiles.foreach(p => namespace.profiles.get(p).foreach { profile =>
                logger.info(s"Applying namespace profile $p")
                context.withProfile(profile)
            })
            context.withEnvironment(namespace.environment)
            context.withConfig(namespace.config)
        }
        context
    }

    private lazy val rootExecutor : RootExecutor = {
        val executor = new RootExecutor(this, rootContext)
        executor
    }

    /**
      * Returns the Namespace tied to this Flowman session.
      * @return
      */
    def namespace : Namespace = _namespace

    /**
      * Returns the Project tied to this Flowman session.
      * @return
      */
    def project : Project = _project

    /**
      * Returns the Spark session tied to this Flowman session. The Spark session will either be created by the
      * Flowman session, or was provided in the builder.
      * @return
      */
    def spark : SparkSession = {
        if (sparkSession == null) {
            synchronized {
                if (sparkSession == null) {
                    sparkSession = createSession()
                }
            }
        }
        sparkSession.get
    }

    /**
      * Returns true if a SparkSession is already available
      * @return
      */
    def sparkRunning: Boolean = sparkSession != null

    /**
     * Returns the root context of this session.
     */
    def context : Context = rootContext

    /**
      * Returns the root executor of this session. Every project has its own derived executor, which should
      * be used instead if working with a project
      *
      * @return
      */
    def executor : Executor = rootExecutor

    /**
      * Creates a new namespace specific context
      *
      * @param project
      * @return
      */
    def getContext(project: Project) : Context = {
        rootContext.getProjectContext(project)
    }

    def getExecutor(project: Project) : Executor = {
        rootExecutor.getProjectExecutor(project)
    }
}
