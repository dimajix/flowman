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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.Catalog
import com.dimajix.flowman.catalog.ExternalCatalog
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.history.NullStateStore
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.metric.MetricSystem
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spi.UdfProvider
import com.dimajix.flowman.storage.NullStore
import com.dimajix.flowman.storage.Store


class SessionBuilder {
    private var _sparkSession: SparkConf => SparkSession = (_ => null)
    private var _sparkMaster = Option(System.getProperty("spark.master")).filter(_.nonEmpty).getOrElse("local[*]")
    private var _sparkName = "Flowman"
    private var _config = Map[String,String]()
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
    def withSparkSession(session:SparkConf => SparkSession) : SessionBuilder = {
        require(session != null)
        _sparkSession = session
        this
    }
    def withSparkSession(session:SparkSession) : SessionBuilder = {
        require(session != null)
        _sparkSession = (_:SparkConf) => session
        this
    }
    def withSparkName(name:String) : SessionBuilder = {
        require(name != null)
        _sparkName = name
        this
    }
    def withSparkMaster(master:String) : SessionBuilder = {
        require(master != null)
        _sparkMaster = master
        this
    }

    /**
      * Adds Spark config variables which actually will override any variables given in specs
      * @param config
      * @return
      */
    def withConfig(config:Map[String,String]) : SessionBuilder = {
        require(config != null)
        _config = _config ++ config
        this
    }

    /**
      * Adds Spark config variables which actually will override any variables given in specs
      * @param key
      * @param value
      * @return
      */
    def withConfig(key:String, value:String) : SessionBuilder = {
        require(key != null)
        require(value != null)
        _config = _config.updated(key, value)
        this
    }

    /**
      * Adds environment variables which actually will override any variables given in specs
      * @param env
      * @return
      */
    def withEnvironment(env:Map[String,String]) : SessionBuilder = {
        require(env != null)
        _environment = _environment ++ env
        this
    }

    /**
      * Adds environment variables which actually will override any variables given in specs
      * @param key
      * @param value
      * @return
      */
    def withEnvironment(key:String,value:String) : SessionBuilder = {
        require(key != null)
        require(value != null)
        _environment = _environment + (key -> value)
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

    def disableSpark() : SessionBuilder = {
        _sparkSession = _ => throw new IllegalStateException("Spark session disable in Flowman session")
        this
    }

    /**
      * Build the Flowman session and applies all previously specified options
      * @return
      */
    def build() : Session = {
        val session = new Session(_namespace, _project, _sparkSession, _sparkMaster, _sparkName, _config, _environment, _profiles, _jars)
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
  * @param _sparkMaster
  * @param _sparkName
  * @param _config
  * @param _environment
  * @param _profiles
  * @param _jars
  */
class Session private[execution](
    _namespace:Namespace,
    _project:Project,
    _sparkSession:SparkConf => SparkSession,
    _sparkMaster:String,
    _sparkName:String,
    _config:Map[String,String],
    _environment: Map[String,String],
    _profiles:Set[String],
    _jars:Set[String]
) {
    require(_jars != null)
    require(_environment != null)
    require(_profiles != null)
    require(_sparkSession != null)
    require(_sparkMaster != null)
    require(_sparkName != null)
    require(_config != null)

    private val logger = LoggerFactory.getLogger(classOf[Session])

    private def sparkJars : Seq[String] = {
        _jars.toSeq
    }

    /**
      * Creates a new Spark Session for this DataFlow session
      *
      * @return
      */
    private def createOrReuseSession() : SparkSession = {
        val sparkConf = this.sparkConf
            .setMaster(_sparkMaster)
            .setAppName(_sparkName)

        Option(_sparkSession)
            .flatMap(builder => Option(builder(sparkConf)))
            .map { injectedSession =>
                logger.info("Creating Spark session using provided builder")
                // Set all session properties that can be changed in an existing session
                sparkConf.getAll.foreach { case (key, value) =>
                    if (!SQLConf.staticConfKeys.contains(key)) {
                        injectedSession.conf.set(key, value)
                    }
                }
                injectedSession
            }
            .getOrElse {
                logger.info("Creating new Spark session")
                val sessionBuilder = SparkSession.builder()
                    .config(sparkConf)
                if (flowmanConf.sparkEnableHive) {
                    logger.info("Enabling Spark Hive support")
                    sessionBuilder.enableHiveSupport()
                }
                sessionBuilder.getOrCreate()
            }
    }
    private def createSession() : SparkSession = {
        val spark = createOrReuseSession()

        // Set checkpoint directory if not already specified
        if (spark.sparkContext.getCheckpointDir.isEmpty) {
            spark.sparkContext.getConf.getOption("spark.checkpoint.dir").foreach(spark.sparkContext.setCheckpointDir)
        }

        // Distribute additional Plugin jar files
        sparkJars.foreach(spark.sparkContext.addJar)

        // Log all config properties
        spark.conf.getAll.toSeq.sortBy(_._1).foreach { case (key, value)=> logger.info("Config: {} = {}", key: Any, value: Any) }

        // Copy all Spark configs over to SparkConf inside the Context
        sparkConf.setAll(spark.conf.getAll)

        // Register special UDFs
        UdfProvider.providers.foreach(_.register(spark.udf))

        spark
    }
    private var sparkSession:SparkSession = null

    private lazy val rootContext : RootContext = {
        def loadProject(name:String) : Option[Project] = {
            Some(store.loadProject(name))
        }

        val builder = RootContext.builder(_namespace, _profiles.toSeq)
            .withEnvironment(_environment, SettingLevel.GLOBAL_OVERRIDE)
            .withConfig(_config, SettingLevel.GLOBAL_OVERRIDE)
            .withProjectResolver(loadProject)
        if (_namespace != null) {
            _profiles.foreach(p => namespace.profiles.get(p).foreach { profile =>
                logger.info(s"Applying namespace profile $p")
                builder.withProfile(profile)
            })
            builder.withEnvironment(namespace.environment)
            builder.withConfig(namespace.config.toMap)
        }
        builder.build()
    }

    private lazy val _configuration = {
        val config = if (_project != null) {
            logger.info("Using project specific configuration settings")
            getContext(_project).config
        }
        else {
            logger.info("Using global configuration settings")
            context.config
        }
        new com.dimajix.flowman.config.Configuration(config)
    }

    private lazy val rootExecutor : RootExecutor = {
        val executor = new RootExecutor(this)
        executor
    }

    private lazy val _externalCatalog : ExternalCatalog = {
        if (_namespace != null && _namespace.catalog != null) {
            _namespace.catalog.instantiate(this)
        }
        else {
            null
        }
    }
    private lazy val _catalog = new Catalog(spark, _externalCatalog)

    private lazy val _projecStore : Store = {
        if (_namespace != null && _namespace.storage != null) {
            _namespace.storage.instantiate(rootContext)
        }
        else {
            new NullStore
        }
    }

    private lazy val _history = {
        if (_namespace != null && _namespace.history != null)
            _namespace.history.instantiate(rootContext)
        else
            new NullStateStore
    }
    private lazy val _runner = {
        if (_namespace != null && _namespace.history != null)
            new MonitoredRunner(_history)
        else
            new SimpleRunner
    }

    private lazy val metricSystem = {
        val system = new MetricSystem
        if (_namespace != null && _namespace.metrics != null) {
            val sink = _namespace.metrics.instantiate(rootContext)
            system.addSink(sink)
        }
        system
    }


    /**
      * Returns the Namespace tied to this Flowman session.
      * @return
      */
    def namespace : Namespace = _namespace

    /**
      * Returns the storage used to manage projects
      * @return
      */
    def store : Store = _projecStore

    /**
      * Returns the Project tied to this Flowman session.
      * @return
      */
    def project : Project = _project

    /**
      * Returns the history store
      * @return
      */
    def history : StateStore = _history

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
      * Returns a Catalog for managing Hive tables
      * @return
      */
    def catalog : Catalog = _catalog

    /**
      * Returns true if a SparkSession is already available
      * @return
      */
    def sparkRunning: Boolean = sparkSession != null

    def config : com.dimajix.flowman.config.Configuration = _configuration
    def flowmanConf : FlowmanConf = _configuration.flowmanConf
    def sparkConf : SparkConf = _configuration.sparkConf
    def hadoopConf : Configuration = _configuration.hadoopConf

    /**
      * Returns the FileSystem as configured in Hadoop
      * @return
      */
    def fs : FileSystem = rootContext.fs

    /**
      * Returns the MetricRegistry of this session
      * @return
      */
    def metrics : MetricSystem = metricSystem

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
      * Returns a new detached Flowman Session sharing the same Spark Context.
      * @param project
      * @return
      */
    def newSession(project:Project) : Session = {
        new Session(
            _namespace,
            project,
            _ => spark.newSession(),
            _sparkMaster,
            _sparkName,
            _config:Map[String,String],
            _environment: Map[String,String],
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

    def shutdown() : Unit = {
        if (sparkSession != null) {
            sparkSession.stop()
            sparkSession = null
        }
    }
}
