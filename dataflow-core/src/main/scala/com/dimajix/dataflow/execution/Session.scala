package com.dimajix.dataflow.execution

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.tools.dfexec.Driver


class Session(sparkName:String, sparkConfig:Seq[(String,String)], environment: Seq[(String,String)]) {
    private val logger = LoggerFactory.getLogger(classOf[Driver])

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

    private lazy val rootContext = {
        val context = new Context(sparkSession)
        context.setEnvironment(environment, true)
        context.setConfig(sparkConfig, true)
        context
    }

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
}
