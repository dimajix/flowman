package com.dimajix.flowman.execution

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project


class SessionBuilder {
    private var _sparkSession: SparkSession = _
    private var _sparkName = ""
    private var _sparkConfig = Map[String,String]()
    private var _environment = Map[String,String]()
    private var _profiles = Set[String]()
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
    def withProfile(profile:String) : SessionBuilder = {
        _profiles = _profiles + profile
        this
    }
    def withProfiles(profile:Seq[String]) : SessionBuilder = {
        _profiles = _profiles ++ profile
        this
    }

    def build() : Session = {
        val session = new Session(_namespace, _sparkSession, _sparkName, _sparkConfig, _environment, _profiles)
        session
    }
}


object Session {
    def builder() = new SessionBuilder
}

class Session private[execution](
    _namespace:Namespace,
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
    private def createSession() = {
        val sparkSession = if(_sparkSession != null) {
            _sparkConfig.foreach(kv => _sparkSession.conf.set(kv._1,kv._2))
            _sparkSession
        }
        else {
            val config = new SparkConf()
                .setAll(_sparkConfig)
                .setAppName(_sparkName)
            SparkSession.builder()
                .config(config)
                .enableHiveSupport()
                .getOrCreate()
        }

        // Register special UDFs
        //udf.register(sparkSession)

        sparkSession.conf.getAll.foreach(kv => logger.info("Config: {} = {}", kv._1: Any, kv._2: Any))
        sparkSession
    }
    private lazy val sparkSession = createSession()

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
        createExecutor(project).context
    }

    def createExecutor(project: Project) : Executor = {
        rootExecutor.getProjectExecutor(project)
    }
}
