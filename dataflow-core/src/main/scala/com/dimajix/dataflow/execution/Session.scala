package com.dimajix.dataflow.execution

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.spec.Namespace
import com.dimajix.dataflow.spec.Project


class SessionBuilder {
    private var _sparkName = ""
    private var _sparkConfig = Map[String,String]()
    private var _environment = Map[String,String]()
    private var _profiles = Set[String]()
    private var _namespace:Namespace = _

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

    def build() : Session = {
        val session = new Session(_namespace, _sparkName, _sparkConfig, _environment, _profiles)
        session
    }
}


object Session {
    def builder() = new SessionBuilder
}

class Session private[execution](
    namespace:Namespace,
    sparkName:String,
    sparkConfig:Map[String,String],
    environment: Map[String,String],
    profiles:Set[String]
) {
    private val logger = LoggerFactory.getLogger(classOf[Session])

    /**
      * Creates a new Spark Session for this DataFlow session
      *
      * @return
      */
    private def createSession() = {
        val config = new SparkConf()
            .setAll(sparkConfig)
            .setAppName(sparkName)
        val sparkSession = SparkSession.builder()
            .config(config)
            .enableHiveSupport()
            .getOrCreate()

        // Register special UDFs
        //udf.register(sparkSession)

        sparkSession.conf.getAll.foreach(kv => logger.info("Config: {} = {}", kv._1: Any, kv._2: Any))

        sparkSession
    }
    private lazy val sparkSession = createSession()

    private lazy val rootContext : RootContext = {
        val context = new RootContext(namespace)
        context.withEnvironment(environment)
        context.withConfig(sparkConfig)
        profiles.foreach(p => namespace.profiles.get(p).foreach { profile =>
            logger.info(s"Applying namespace profile $p")
            context.withProfile(profile)
        })
        context.withEnvironment(namespace.environment)
        context.withConfig(namespace.config)
        context
    }

    def context : Context = rootContext

    /**
      * Creates a new namespace specific context
      *
      * @param project
      * @param profiles
      * @return
      */
    def createContext(project: Project, profiles:Seq[String]) : Context = {
        // Apply all active profiles
        val profileContext = profiles
            .map(name => (name,project.profiles(name)))
            .foldLeft(rootContext){case (context,(name,profile)) =>
                logger.info(s"Applying profile $name to context")
                context.withProfile(profile)
            }
        // Finally set additional values
        val context = profileContext
            .withEnvironment(project.environment)
            .withConfig(project.config)

        // Print current environment variables
        context.environment.foreach(kv => logger.info("Environment: {} = {}", kv._1: Any, kv._2: Any))
        context
    }

    def createExecutor(project: Project, profiles:Seq[String]) : Executor = ???
}
