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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.slf4j.LoggerFactory

import com.dimajix.flowman.namespace.Namespace
import com.dimajix.flowman.namespace.monitor.Monitor
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spi.UdfProvider


class SessionBuilder {
    private var _sparkSession: SparkSession = _
    private var _sparkName = ""
    private var _sparkConfig = Map[String,String]()
    private var _environment = Seq[(String,String)]()
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
        require(session != null)
        _sparkSession = session
        this
    }
    def withSparkName(name:String) : SessionBuilder = {
        require(name != null)
        _sparkName = name
        this
    }

    /**
      * Adds Spark config variables which actually will override any variables given in specs
      * @param config
      * @return
      */
    def withSparkConfig(config:Map[String,String]) : SessionBuilder = {
        require(config != null)
        _sparkConfig = _sparkConfig ++ config
        this
    }

    /**
      * Adds Spark config variables which actually will override any variables given in specs
      * @param config
      * @return
      */
    def withSparkConfig(key:String,value:String) : SessionBuilder = {
        require(key != null)
        require(value != null)
        _sparkConfig = _sparkConfig.updated(key, value)
        this
    }

    /**
      * Adds environment variables which actually will override any variables given in specs
      * @param env
      * @return
      */
    def withEnvironment(env:Seq[(String,String)]) : SessionBuilder = {
        require(env != null)
        _environment = _environment ++ env
        this
    }

    /**
      * Adds environment variables which actually will override any variables given in specs
      * @param env
      * @return
      */
    def withEnvironment(key:String,value:String) : SessionBuilder = {
        require(key != null)
        require(value != null)
        _environment = _environment :+ (key -> value)
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
        require(profile != null)
        _profiles = _profiles + profile
        this
    }

    /**
      * Adds a list of profile names to be activated. This does not remove any previously activated profile
      * @param profiles
      * @return
      */
    def withProfiles(profiles:Seq[String]) : SessionBuilder = {
        require(profiles != null)
        _profiles = _profiles ++ profiles
        this
    }

    /**
      * Adds JAR files to this session which will be distributed to all Spark nodes
      * @param jars
      * @return
      */
    def withJars(jars:Seq[String]) : SessionBuilder = {
        require(jars != null)
        _jars = _jars ++ jars
        this
    }

    /**
      * Build the Flowman session and applies all previously specified options
      * @return
      */
    def build() : Session = {
        val session = new Session(_namespace, _project, () => _sparkSession, _sparkName, _sparkConfig, _environment, _profiles, _jars)
        session
    }
}


object Session {
    def builder() = new SessionBuilder
}


/**
  * A Flowman session is used as the starting point for executing data flows. It contains information about the
  * Namespace, the Project and also managed a Spark session.
  *
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
                                        _sparkSession:() => SparkSession,
                                        _sparkName:String,
                                        _sparkConfig:Map[String,String],
                                        _environment: Seq[(String,String)],
                                        _profiles:Set[String],
                                        _jars:Set[String]
                                    ) {
    require(_jars != null)
    require(_environment != null)
    require(_profiles != null)
    require(_sparkSession != null)
    require(_sparkName != null)
    require(_sparkConfig != null)

    private val logger = LoggerFactory.getLogger(classOf[Session])

    private val _monitor = {
        if (_namespace != null && _namespace.monitor != null)
            _namespace.monitor
        else
            null
    }
    private val _runner = {
        if (_monitor  != null)
            new MonitoredRunner(_monitor)
        else
            new SimpleRunner
    }


    private def sparkConfig : Map[String,String] = {
        if (_project != null) {
            logger.info("Using project specific Spark configuration settings")
            getContext(_project).config // ++ _sparkConfig
        }
        else {
            logger.info("Using global Spark configuration settings")
            context.config // ++ _sparkConfig
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
    private def createOrReuseSession() : SparkSession = {
        val injectedSession = _sparkSession()
        if (injectedSession != null) {
            logger.info("Reusing provided Spark session")
            // Set all session properties that can be changed in an existing session
            sparkConfig.foreach { case (key, value) =>
                if (!SQLConf.staticConfKeys.contains(key)) {
                    injectedSession.conf.set(key, value)
                }
            }
            injectedSession
        }
        else {
            logger.info("Creating new Spark session")
            val sparkConf = context.sparkConf
                .setAppName(_sparkName)
                .setAll(sparkConfig.toSeq)
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
    }
    private def createSession() : SparkSession = {
        val spark = createOrReuseSession()

        // Distribute additional Plugin jar files
        sparkJars.foreach(spark.sparkContext.addJar)

        // Log all config properties
        spark.conf.getAll.toSeq.sortBy(_._1).foreach { case (key, value)=> logger.info("Config: {} = {}", key: Any, value: Any) }

        // Copy all Spark configs over to SparkConf inside the Context
        context.sparkConf.setAll(spark.conf.getAll)

        // Register special UDFs
        UdfProvider.providers.foreach(_.register(spark.udf))

        spark
    }
    private var sparkSession:SparkSession = null

    private lazy val rootContext : Context = {
        val builder = RootContext.builder(_namespace, _profiles.toSeq)
            .withRunner(_runner)
            .withEnvironment(_environment, SettingLevel.GLOBAL_OVERRIDE)
            .withConfig(_sparkConfig, SettingLevel.GLOBAL_OVERRIDE)
        if (_namespace != null) {
            _profiles.foreach(p => namespace.profiles.get(p).foreach { profile =>
                logger.info(s"Applying namespace profile $p")
                builder.withProfile(profile)
            })
            builder.withEnvironment(namespace.environment)
            builder.withConfig(namespace.config.toMap)
        }
        builder.build().asInstanceOf[RootContext]
    }

    private lazy val rootExecutor : RootExecutor = {
        val executor = new RootExecutor(this, rootContext)
        executor
    }

    def monitor : Monitor = _monitor

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
      * Returns the appropriate runner
      *
      * @return
      */
    def runner : Runner = _runner

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
        sparkSession
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
      * Either returns an existing or creates a new project specific context
      *
      * @param project
      * @return
      */
    def getContext(project: Project) : Context = {
        rootContext.getProjectContext(project)
    }

    /**
      * Either returns an existing or creates a new project specific executor
      *
      * @param project
      * @return
      */
    def getExecutor(project: Project) : Executor = {
        rootExecutor.getProjectExecutor(project)
    }

    /**
      * Returns a new detached Flowman Session sharing the same Spark Context.
      * @param project
      * @return
      */
    def newSession(project:Project) : Session = {
        new Session(
            _namespace,
            project,
            () => spark.newSession(),
            _sparkName,
            _sparkConfig:Map[String,String],
            _environment: Seq[(String,String)],
            _profiles:Set[String],
            Set()
        )
    }

    /**
      * Returns a new detached Flowman Session for the same namespace and project sharing the same Spark Context.
      * @return
      */
    def newSession() : Session = {
        newSession(_project)
    }
}
