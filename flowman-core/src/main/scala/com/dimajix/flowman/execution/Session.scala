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

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkShim
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.Catalog
import com.dimajix.flowman.config.Configuration
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.history.NullStateStore
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.metric.MetricSystem
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.spi.LogFilter
import com.dimajix.flowman.spi.SparkExtension
import com.dimajix.flowman.spi.UdfProvider
import com.dimajix.flowman.storage.NullStore
import com.dimajix.flowman.storage.Store
import com.dimajix.spark.sql.catalyst.optimizer.ExtraOptimizations
import com.dimajix.spark.sql.execution.ExtraStrategies


object Session {
    class Builder {
        private var sparkSession: SparkConf => SparkSession = (_ => null)
        private var sparkMaster:Option[String] = None
        private var sparkName:Option[String] = None
        private var config = Map[String,String]()
        private var environment = Map[String,String]()
        private var profiles = Set[String]()
        private var project:Option[Project] = None
        private var namespace:Option[Namespace] = None
        private var jars = Set[String]()

        /**
         * Injects an existing Spark session. If no session is provided, Flowman will create its own Spark session
         * @param session
         * @return
         */
        def withSparkSession(session:SparkConf => SparkSession) : Builder = {
            require(session != null)
            sparkSession = session
            this
        }
        def withSparkSession(session:SparkSession) : Builder = {
            require(session != null)
            sparkSession = (_:SparkConf) => session
            this
        }
        def withSparkName(name:String) : Builder = {
            require(name != null)
            sparkName = Some(name)
            this
        }
        def withSparkMaster(master:String) : Builder = {
            require(master != null)
            sparkMaster = Some(master)
            this
        }

        /**
         * Adds Spark config variables which actually will override any variables given in specs
         * @param config
         * @return
         */
        def withConfig(config:Map[String,String]) : Builder = {
            require(config != null)
            this.config = this.config ++ config
            this
        }

        /**
         * Adds Spark config variables which actually will override any variables given in specs
         * @param key
         * @param value
         * @return
         */
        def withConfig(key:String, value:String) : Builder = {
            require(key != null)
            require(value != null)
            config = config.updated(key, value)
            this
        }

        /**
         * Adds environment variables which actually will override any variables given in specs
         * @param env
         * @return
         */
        def withEnvironment(env:Map[String,String]) : Builder = {
            require(env != null)
            environment = environment ++ env
            this
        }

        /**
         * Adds environment variables which actually will override any variables given in specs
         * @param key
         * @param value
         * @return
         */
        def withEnvironment(key:String,value:String) : Builder = {
            require(key != null)
            require(value != null)
            environment = environment + (key -> value)
            this
        }

        /**
         * Adds a Namespace to source more configuration from
         * @param namespace
         * @return
         */
        def withNamespace(namespace:Namespace) : Builder = {
            this.namespace = Some(namespace)
            this
        }

        /**
         * Adds a project to source more configurations from.
         * @param project
         * @return
         */
        def withProject(project:Project) : Builder = {
            this.project = Some(project)
            this
        }

        /**
         * Adds a new profile to be activated
         * @param profile
         * @return
         */
        def withProfile(profile:String) : Builder = {
            require(profile != null)
            profiles = profiles + profile
            this
        }

        /**
         * Adds a list of profile names to be activated. This does not remove any previously activated profile
         * @param profiles
         * @return
         */
        def withProfiles(profiles:Iterable[String]) : Builder = {
            require(profiles != null)
            this.profiles = this.profiles ++ profiles
            this
        }

        /**
         * Adds JAR files to this session which will be distributed to all Spark nodes
         * @param jars
         * @return
         */
        def withJars(jars:Iterable[String]) : Builder = {
            require(jars != null)
            this.jars = this.jars ++ jars
            this
        }

        def disableSpark() : Builder = {
            sparkSession = _ => throw new IllegalStateException("Spark session disable in Flowman session")
            this
        }

        /**
         * Build the Flowman session and applies all previously specified options
         * @return
         */
        def build() : Session = {
            new Session(namespace, project, sparkSession, sparkMaster, sparkName, config, environment, profiles, jars)
        }
    }

    def builder() = new Builder
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
    _namespace:Option[Namespace],
    _project:Option[Project],
    _sparkSession:SparkConf => SparkSession,
    _sparkMaster:Option[String],
    _sparkName:Option[String],
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
    private def sparkMaster : String = {
        // How should priorities look like?
        //   1. Spark master from Flowman config.
        //   2. Spark master from application code / session builder
        //   3. Spark master from spark-submit. This is to be found in SparkConf
        //   4. Default master
        config.toMap.get("spark.master")
            .filter(_.nonEmpty)
            .orElse(_sparkMaster)
            .filter(_.nonEmpty)
            .orElse(sparkConf.getOption("spark.master"))
            .filter(_.nonEmpty)
            .getOrElse("local[*]")
    }
    private def sparkName : String = {
        // How should priorities look like?
        //   1. Spark app name from Flowman config.
        //   2. Spark app name from application code
        //   3. Spark app name from spark-submit / command-line
        //   4. Spark app name from Flowman project
        //   5. Default Spark app name
        config.toMap.get("spark.app.name")
            .filter(_.nonEmpty)
            .orElse(_sparkName)
            .filter(_.nonEmpty)
            .orElse(_project.map(_.name).filter(_.nonEmpty).map("Flowman - " + _))
            .orElse(sparkConf.getOption("spark.app.name"))
            .filter(_.nonEmpty)
            .getOrElse("Flowman")
    }

    /**
      * Creates a new Spark Session for this DataFlow session
      *
      * @return
      */
    private def createOrReuseSession() : SparkSession = {
        val sparkConf = this.sparkConf
            .setMaster(sparkMaster)
            .setAppName(sparkName)

        Option(_sparkSession)
            .flatMap(builder => Option(builder(sparkConf)))
            .map { spark =>
                logger.info("Creating Spark session using provided builder")
                // Set all session properties that can be changed in an existing session
                sparkConf.getAll.foreach { case (key, value) =>
                    if (!SparkShim.isStaticConf(key)) {
                        spark.conf.set(key, value)
                    }
                }
                spark
            }
            .getOrElse {
                logger.info("Creating new Spark session")
                val sessionBuilder = SparkSession.builder()
                    .config(sparkConf)
                if (flowmanConf.sparkEnableHive) {
                    logger.info("Enabling Spark Hive support")
                    sessionBuilder.enableHiveSupport()
                }
                // Apply all session extensions to builder
                SparkExtension.extensions.foldLeft(sessionBuilder)((builder,ext) => ext.register(builder, config))
                // Create Spark session
                sessionBuilder.getOrCreate()
            }
    }
    private def createSession() : SparkSession = {
        val spark = createOrReuseSession()

        // Set checkpoint directory if not already specified
        if (spark.sparkContext.getCheckpointDir.isEmpty) {
            spark.sparkContext.getConf.getOption("spark.checkpoint.dir").foreach(spark.sparkContext.setCheckpointDir)
        }

        if (flowmanConf.getConf(FlowmanConf.SPARK_EAGER_CACHE)) {
            ExtraOptimizations.enableEagerCache(spark)
        }

        // Register additional planning strategies
        ExtraStrategies.register(spark)

        // Apply all session extensions
        SparkExtension.extensions.foreach(_.register(spark, config))

        // Register special UDFs
        UdfProvider.providers.foreach(_.register(spark.udf, config))

        // Distribute additional Plugin jar files
        sparkJars.foreach(spark.sparkContext.addJar)

        // Log all config properties
        val logFilters = LogFilter.filters
        spark.conf.getAll.toSeq.sortBy(_._1).foreach { keyValue =>
            logFilters.foldLeft(Option(keyValue))((kv, f) => kv.flatMap(kv => f.filterConfig(kv._1,kv._2)))
                .foreach { case (key,value) => logger.info("Config: {} = {}", key: Any, value: Any) }
        }

        // Copy all Spark configs over to SparkConf inside the Context
        sparkConf.setAll(spark.conf.getAll)

        spark
    }
    private var sparkSession:SparkSession = null

    private lazy val rootContext : RootContext = {
        def loadProject(name:String) : Option[Project] = {
            Some(store.loadProject(name))
        }

        val builder = RootContext.builder(_namespace, _profiles)
            .withEnvironment(_environment, SettingLevel.GLOBAL_OVERRIDE)
            .withConfig(_config, SettingLevel.GLOBAL_OVERRIDE)
            .withProjectResolver(loadProject)
        _namespace.foreach { ns =>
            _profiles.foreach(p => ns.profiles.get(p).foreach { profile =>
                logger.info(s"Applying namespace profile $p")
                builder.withProfile(profile)
            })
            builder.withEnvironment(ns.environment)
            builder.withConfig(ns.config)
        }
        builder.build()
    }

    private lazy val _configuration : Configuration = {
        if (_project.nonEmpty) {
            logger.info("Using project specific configuration settings")
            getContext(_project.get).config
        }
        else {
            logger.info("Using global configuration settings")
            context.config
        }
    }

    private lazy val rootExecution : RootExecution = {
        new RootExecution(this)
    }

    private lazy val _catalog = {
        val externalCatalogs = _namespace.toSeq.flatMap(_.catalogs).map(_.instantiate(rootContext))
        new Catalog(spark, config, externalCatalogs)
    }

    private lazy val _projectStore : Store = {
        _namespace.flatMap(_.store).map(_.instantiate(rootContext)).getOrElse(new NullStore)
    }

    private lazy val _history = {
        _namespace.flatMap(_.history)
            .map(_.instantiate(rootContext))
            .getOrElse(new NullStateStore())
    }
    private lazy val _hooks = {
        _namespace.toSeq.flatMap(_.hooks)
    }
    private lazy val metricSystem = {
        val system = new MetricSystem
        _namespace.toSeq.flatMap(_.metrics)
            .map(_.instantiate(rootContext))
            .foreach(system.addSink)
        system
    }


    /**
      * Returns the Namespace tied to this Flowman session.
      * @return
      */
    def namespace : Option[Namespace] = _namespace

    /**
     * Returns the Project tied to this Flowman session.
     * @return
     */
    def project : Option[Project] = _project

    /**
     * Returns the list of active profile names
     *
     * @return
     */
    def profiles: Set[String] = _profiles

    /**
      * Returns the storage used to manage projects
      * @return
      */
    def store : Store = _projectStore

    /**
      * Returns the history store
      * @return
      */
    def history : StateStore = _history

    /**
     * Returns the list of all hooks
     */
    def hooks : Seq[Template[Hook]] = _hooks

    /**
      * Returns an appropriate runner for a specific job
      *
      * @return
      */
    def runner : Runner = {
        new Runner(execution, _history, _hooks)
    }

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

    def config : Configuration = _configuration
    def flowmanConf : FlowmanConf = _configuration.flowmanConf
    def sparkConf : SparkConf = _configuration.sparkConf
    def hadoopConf : HadoopConf = _configuration.hadoopConf

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
      * Returns the root execution of this session. You might want to wrap it up into a [[ScopedExecution]] to
      * isolate resources.
      * @return
      */
    def execution : Execution = rootExecution

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
        require(project != null)
        new Session(
            _namespace,
            Some(project),
            _ => spark.newSession(),
            _sparkMaster,
            _sparkName,
            _config,
            _environment,
            _profiles,
            Set()
        )
    }

    /**
      * Returns a new detached Flowman Session for the same namespace and project sharing the same Spark Context.
      * @return
      */
    def newSession() : Session = {
        new Session(
            _namespace,
            _project,
            _ => spark.newSession(),
            _sparkMaster,
            _sparkName,
            _config,
            _environment,
            _profiles,
            Set()
        )
    }

    def shutdown() : Unit = {
        if (sparkSession != null) {
            sparkSession.stop()
            sparkSession = null
        }
    }
}
