/*
 * Copyright (C) 2018 The Flowman Authors
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

import scala.collection.mutable

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkShim
import org.slf4j.ILoggerFactory
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.HiveCatalog
import com.dimajix.flowman.config.Configuration
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.documentation.Documenter
import com.dimajix.flowman.execution.Session.builder
import com.dimajix.flowman.fs.FileSystem
import com.dimajix.flowman.history.NullStateStore
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.metric.MetricSystem
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spi.LogFilter
import com.dimajix.flowman.spi.SparkExtension
import com.dimajix.flowman.spi.UdfProvider
import com.dimajix.flowman.storage.NullStore
import com.dimajix.flowman.storage.Store
import com.dimajix.spark.sql.catalyst.optimizer.ExtraOptimizations
import com.dimajix.spark.sql.execution.ExtraStrategies


object Session {
    private val logger = LoggerFactory.getLogger(classOf[Session])
    private def stopSparkSession(spark:SparkSession) : Unit = {
        logger.info("Stopping Spark session")
        spark.stop()
    }

    final class Builder private[execution](parent:Option[Session]) {
        private var createSparkSession:SparkSession.Builder => SparkSession = null
        private var stopSparkSession:SparkSession => Unit = null
        private var sparkMaster:Option[String] = None
        private var sparkName:Option[String] = None
        private var config = Map[String,String]()
        private var environment = Map[String,String]()
        private var profiles = Set[String]()
        private var project:Option[Project] = None
        private var store:Option[Store] = None
        private var namespace:Option[Namespace] = None
        private var jars = Set[String]()
        private var listeners = Seq[ExecutionListener]()
        private var loggerFactory:Option[ILoggerFactory] = None

        /**
         * Injects a builder for a Spark session. The builder will be lazily called, when required. Note that
         * [[Session.shutdown()]] will also stop the constructed session.
         * @param session
         * @return
         */
        def withSparkSession(session:SparkSession.Builder => SparkSession) : Builder = {
            require(session != null)
            requireNoParent()
            createSparkSession = session
            stopSparkSession = Session.stopSparkSession
            this
        }
        /**
         * Injects an existing Spark session, which will also not be stopped by Flowman. That means a call to
         * [[Session.shutdown()]] will NOT stop the provided Spark session.
         * @param session
         * @return
         */
        def withSparkSession(session:SparkSession) : Builder = {
            require(session != null)
            requireNoParent()
            createSparkSession = _ => session
            stopSparkSession = _ => Unit
            this
        }
        def withSparkName(name:String) : Builder = {
            require(name != null)
            requireNoParent()
            sparkName = Some(name)
            this
        }
        def withSparkMaster(master:String) : Builder = {
            require(master != null)
            requireNoParent()
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

        def withConfig(key:String, value:Boolean) : Builder = {
            withConfig(key, value.toString)
        }

        def withConfig(key:String, value:Int) : Builder = {
            withConfig(key, value.toString)
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

        def withStore(store:Store) : Builder = {
            this.store = Some(store)
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
            requireNoParent()
            this.jars = this.jars ++ jars
            this
        }

        /**
         * Disables Spark in this Flowman session. Only useful for some limited unittests.
         * @return
         */
        def disableSpark() : Builder = {
            requireNoParent()
            createSparkSession = _ => throw new IllegalStateException("Spark session disable in Flowman session")
            stopSparkSession = _ => throw new IllegalStateException("Spark session disable in Flowman session")
            this
        }

        /**
         * Enables Spark in this Flowman session. The session itself will take over responsibility of creating and
         * shutting down the Spark session.
         * @return
         */
        def enableSpark() : Builder = {
            requireNoParent()
            createSparkSession = builder => builder.getOrCreate()
            stopSparkSession = Session.stopSparkSession
            this
        }

        def withListener(listener:ExecutionListener) : Builder = {
            this.listeners = this.listeners :+ listener
            this
        }

        def withLoggerFactory(loggerFactory: ILoggerFactory) : Builder = {
            this.loggerFactory = Some(loggerFactory)
            this
        }

        /**
         * Build the Flowman session and applies all previously specified options
         * @return
         */
        def build() : Session = {
            if (createSparkSession == null && parent.isEmpty)
                throw new IllegalArgumentException("You need to either enable or disable Spark before creating a Flowman Session.")

            new Session(
                namespace.orElse(parent.flatMap(_._namespace)),
                project,
                store,
                parent.map(p => (_:SparkSession.Builder) => p.spark.newSession()).getOrElse(createSparkSession),
                if (parent.nonEmpty) ((_:SparkSession) => Unit) else stopSparkSession,
                parent.map(_._sparkMaster).getOrElse(sparkMaster),
                parent.map(_._sparkName).getOrElse(sparkName),
                parent.map(_._config).getOrElse(Map()) ++ config,
                parent.map(_._environment).getOrElse(Map()) ++ environment,
                parent.map(_._profiles).getOrElse(Set()) ++ profiles,
                parent.map(_ => Set[String]()).getOrElse(jars),
                listeners.map(l => (l,None)),
                loggerFactory.getOrElse(LoggerFactory.getILoggerFactory)
            )
        }

        private def requireNoParent(): Unit = {
            if (parent.nonEmpty)
                throw new IllegalArgumentException("Cannot configure SparkSession for Flowman Session with parent session")
        }
    }

    def builder() = new Builder(None)
    def builder(parent:Session) = new Builder(Some(parent))
    def builder(project:Project) = new Builder(None).withProject(project)
}


/**
  * A Flowman session is used as the starting point for executing data flows. It contains information about the
  * Namespace, the Project and also managed a Spark session.
  *
  * @param _namespace
  * @param _project
  * @param _createSparkSession
  * @param _sparkMaster
  * @param _sparkName
  * @param _config
  * @param _environment
  * @param _profiles
  * @param _jars
  */
final class Session private[execution](
    private[execution] val _namespace:Option[Namespace],
    private[execution] val _project:Option[Project],
    private[execution] val _store:Option[Store],
    private[execution] val _createSparkSession:SparkSession.Builder => SparkSession,
    private[execution] val _stopSparkSession:SparkSession => Unit,
    private[execution] val _sparkMaster:Option[String],
    private[execution] val _sparkName:Option[String],
    private[execution] val _config:Map[String,String],
    private[execution] val _environment: Map[String,String],
    private[execution] val _profiles:Set[String],
    private[execution] val _jars:Set[String],
    private[execution] val _listeners:Seq[(ExecutionListener,Option[Token])],
    private[execution] val _loggerFactory:ILoggerFactory
) {
    private val logger = _loggerFactory.getLogger(classOf[Session].getName)

    private def sparkJars : Seq[String] = {
        _jars.toSeq
    }
    private def sparkMaster : String = {
        // How should priorities look like?
        //   1. Spark master from application code / session builder
        //   2. Spark master from Flowman config.
        //   3. Spark master from spark-submit. This is to be found in SparkConf
        //   4. Default master
        _sparkMaster.filter(_.nonEmpty)
            .orElse(config.getOption("spark.master"))
            .filter(_.nonEmpty)
            .orElse(sparkConf.getOption("spark.master"))
            .filter(_.nonEmpty)
            .getOrElse("local[*]")
    }
    private def sparkName : String = {
        // How should priorities look like?
        //   1. Spark app name from application code
        //   2. Spark app name from Flowman config.
        //   3. Spark app name from spark-submit / command-line
        //   4. Spark app name from Flowman project
        //   5. Default Spark app name
        _sparkName
            .filter(_.nonEmpty)
            .orElse(config.getOption("spark.app.name"))
            .filter(_.nonEmpty)
            .orElse(_project.map(_.name).filter(_.nonEmpty).map("Flowman - " + _))
            .orElse(sparkConf.getOption("spark.app.name"))
            .filter(_.nonEmpty)
            .getOrElse("Flowman")
    }

    /**
      * Creates a new Spark Session for this Flowman session
      *
      * @return
      */
    private def createSession() : SparkSession = {
        logger.info("Creating new Spark session")
        val sessionBuilder = SparkSession.builder()
            .config(sparkConf)
            .appName(sparkName)
            .master(sparkMaster)
        if (flowmanConf.sparkEnableHive) {
            logger.info("Enabling Spark Hive support")
            sessionBuilder.enableHiveSupport()
        }

        // Apply all session extensions to builder
        SparkExtension.extensions.foldLeft(sessionBuilder)((builder, ext) => ext.register(builder, config))

        val spark = _createSparkSession(sessionBuilder)
        // Set all session properties that can be changed in an existing session
        sparkConf.getAll.foreach { case (key, value) =>
            if (!SparkShim.isStaticConf(key)) {
                spark.conf.set(key, value)
            }
        }

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
        logger.info("Spark configuration:")
        val logFilters = LogFilter.filters
        spark.conf.getAll.toSeq
            .sortBy(_._1)
            .foreach { keyValue =>
                logFilters.foldLeft(Option(keyValue))((kv, f) => kv.flatMap(kv => f.filterConfig(kv._1,kv._2)))
                    .foreach { case (key,value) => logger.info("  {} = {}", key: Any, value: Any) }
            }

        // Copy all Spark configs over to SparkConf inside the Context
        sparkConf.setAll(spark.conf.getAll)

        spark
    }
    private var sparkSession:SparkSession = null

    private var sessionCleaner:SessionCleaner = null

    private val rootExecution : RootExecution = new RootExecution(this)
    private val operationsManager = new ActivityManager

    private def loadProjects() : Seq[Project] = {
        val allProjects = mutable.Map[String,Project] ()
        def load(p:String, name:String) : Project = {
            logger.info(s"Importing project '$name' as dependency of project '$p'")
            val project = allProjects.getOrElseUpdate(name, store.loadProject(name))
            project.imports.foreach(i => load(project.name, i.project))
            project
        }

        project.foreach { p =>
            allProjects.put(p.name, p)
            p.imports.foreach(i => load(p.name, i.project))
        }
        allProjects.values.toSeq
    }

    private lazy val namespaceContext : RootContext = {
        val builder = RootContext.builder(_namespace, _profiles)
            .withLoggerFactory(loggerFactory)
            .withEnvironment(_environment, SettingLevel.GLOBAL_OVERRIDE)
            .withConfig(_config, SettingLevel.GLOBAL_OVERRIDE)
        _namespace.foreach { ns =>
            _profiles.foreach(p => ns.profiles.get(p).foreach { profile =>
                logger.info(s"Activating namespace profile '$p'")
                builder.withProfile(profile)
            })
            builder.withEnvironment(ns.environment)
            builder.withConfig(ns.config)
        }
        builder.build()
    }
    private lazy val rootContext : RootContext = {
        val builder = RootContext.builder(namespaceContext)
            .withExecution(rootExecution)
            .withLoggerFactory(loggerFactory)
            .withProjects(loadProjects())
        _project.foreach { prj =>
            // github-155: Apply project configuration to session
            _profiles.foreach(p => prj.profiles.get(p).foreach { profile =>
                logger.info(s"Activating project profile '$p'")
                builder.withConfig(profile.config, SettingLevel.PROJECT_PROFILE)
            })
            builder.withConfig(prj.config, SettingLevel.PROJECT_SETTING)
        }
        builder.build()
    }

    private lazy val _configuration : Configuration = {
        val conf = if (_project.nonEmpty) {
            logger.info("Using project specific configuration settings")
            getContext(_project.get).config
        }
        else {
            logger.info("Using global configuration settings")
            context.config
        }

        // Log Flowman configuration
        logger.info("Flowman configuration:")
        conf.flowmanConf.getAll.toSeq
            .sortBy(_._1)
            .foreach { case(key,value) =>
                logger.info("  {} = {}", key: Any, value: Any)
            }
        logger.info("")

        conf
    }

    private lazy val _catalog = {
        val externalCatalogs = _namespace.toSeq.flatMap(_.catalogs).map(_.instantiate(namespaceContext))
        new HiveCatalog(spark, config, externalCatalogs)
    }

    private lazy val _projectStore : Store = {
        _store.orElse(_namespace.flatMap(_.store).map(_.instantiate(namespaceContext))).getOrElse(new NullStore)
    }

    private lazy val _history = {
        _namespace.flatMap(_.history)
            .map(_.instantiate(namespaceContext))
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
      * Returns the [[Namespace]] tied to this Flowman session.
      * @return
      */
    def namespace : Option[Namespace] = _namespace

    /**
     * Returns the [[Project]] tied to this Flowman session.
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
    def hooks : Seq[Prototype[Hook]] = _hooks

    /**
     * Returns list of listeners which are active in this Session
     * @return
     */
    def listeners : Seq[(ExecutionListener,Option[Token])] = _listeners

    /**
      * Returns an appropriate runner for a specific job. Note that every invocation will actually create a new
      * runner.
      *
      * @return
      */
    def runner : Runner = {
        new Runner(execution, _history, _hooks)
    }

    /**
     * Returns the [[SessionCleaner]] associated with this session
     * @return
     */
    def cleaner : SessionCleaner = {
        if (sessionCleaner == null) {
            synchronized {
                if (sessionCleaner == null) {
                    sessionCleaner = new SessionCleaner(this)
                    sessionCleaner.start()
                }
            }
        }
        sessionCleaner
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
      * Returns a [[HiveCatalog]] for managing Hive tables
      * @return
      */
    def catalog : HiveCatalog = _catalog

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
      * Returns the [[MetricRegistry]] of this Flowman session
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
     * Returns the [[ActivityManager]] of this session, where all background activities and streaming queries are
     * managed.
     *
     * @return
     */
    def activities: ActivityManager = operationsManager

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
     * Returns a session specific logger factory
     * @return
     */
    def loggerFactory : ILoggerFactory = _loggerFactory

    /**
     * Returns a new detached Flowman Session sharing the same Spark Context.
     * @param project
     * @return
     */
    def newSession(project:Project, store:Store) : Session = {
        builder(this)
            .withProject(project)
            .withStore(store)
            .build()
    }

    /**
      * Returns a new detached Flowman Session sharing the same Spark Context.
      * @param project
      * @return
      */
    def newSession(project:Project) : Session = {
        builder(this)
            .withProject(project)
            .build()
    }

    /**
      * Returns a new detached Flowman Session for the same namespace and project sharing the same Spark Context.
      * @return
      */
    def newSession() : Session = {
        builder(this)
            .build()
    }

    def shutdown() : Unit = {
        if (sessionCleaner != null) {
            sessionCleaner.stop()
            sessionCleaner = null
        }
        if (sparkSession != null) {
            _stopSparkSession(sparkSession)
            sparkSession = null
        }
    }
}
