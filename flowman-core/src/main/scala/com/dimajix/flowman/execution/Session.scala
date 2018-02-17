package com.dimajix.flowman.execution

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.util.SparkUtils


class SessionBuilder {
    private var _sparkSession: SparkSession = _
    private var _sparkName = ""
    private var _sparkConfig = Map[String,String]()
    private var _environment = Map[String,String]()
    private var _profiles = Set[String]()
    private var _project:Project = _
    private var _namespace:Namespace = _

    def withSparkSession(session:SparkSession) = {
        _sparkSession = session
        this
    }
    def withSparkName(name:String) : SessionBuilder = {
        _sparkName = name
        this
    }
    def withSparkConfig(config:Map[String,String]) : SessionBuilder = {
        _sparkConfig = _sparkConfig ++ config
        this
    }
    def withEnvironment(env:Map[String,String]) : SessionBuilder = {
        _environment = _environment ++ env
        this
    }
    def withNamespace(namespace:Namespace) : SessionBuilder = {
        _namespace = namespace
        this
    }
    def withProject(project:Project) : SessionBuilder = {
        _project = project
        this
    }
    def withProfile(profile:String) : SessionBuilder = {
        _profiles = _profiles + profile
        this
    }
    def withProfiles(profile:Seq[String]) : SessionBuilder = {
        _profiles = _profiles ++ profile
        this
    }

    def build() : Session = {
        val session = new Session(_namespace, _project, _sparkSession, _sparkName, _sparkConfig, _environment, _profiles)
        session
    }
}


object Session {
    def builder() = new SessionBuilder
}

class Session private[execution](
    _namespace:Namespace,
    _project:Project,
    _sparkSession:SparkSession,
    _sparkName:String,
    _sparkConfig:Map[String,String],
    _environment: Map[String,String],
    _profiles:Set[String]
) {
    private val logger = LoggerFactory.getLogger(classOf[Session])

    /**
      * Creates a new Spark Session for this DataFlow session
      *
      * @return
      */


    private def createOrReuseSession() : Option[SparkSession] = {
        if(_sparkSession != null) {
            logger.info("Reusing existing Spark session")
            _sparkConfig.foreach(kv => _sparkSession.conf.set(kv._1,kv._2))
            Some(_sparkSession)
        }
        else {
            logger.info("Creating new Spark session")
            Try {
                val config = new SparkConf()
                    .setAppName(_sparkName)
                val master = System.getProperty("spark.master")
                if (master == null || master.isEmpty) {
                    logger.info("No Spark master specified - using local[*]")
                    config.setMaster("local[*]")
                    config.set("spark.sql.shuffle.partitions", "16")
                }
                SparkSession.builder()
                    .config(config)
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
    private def createSession(config:Map[String,String]) = {
        val sparkSession = createOrReuseSession()
        val mergedConfig = if (_project != null) {
            logger.info("Using project specific Spark configuration settings")
            createContext(_project).config
        }
        else {
            logger.info("Using global Spark configuration settings")
            context.config
        }

        sparkSession.foreach(spark => {
            SparkUtils.configure(spark, mergedConfig)
            spark.conf.getAll.toSeq.sortBy(_._1).foreach { case (key, value)=> logger.info("Config: {} = {}", key: Any, value: Any) }
        })

        // Register special UDFs
        //udf.register(sparkSession)

        sparkSession
    }
    private lazy val sparkSession = createSession(rootContext.config)

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
        val executor = new RootExecutor(rootContext, () => sparkSession)
        executor
    }

    def namespace : Namespace = _namespace

    def project : Project = _project

    def spark : SparkSession = sparkSession.get

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
    def createContext(project: Project) : Context = {
        rootContext.getProjectContext(project)
    }

    def createExecutor(project: Project) : Executor = {
        rootExecutor.getProjectExecutor(project)
    }
}
