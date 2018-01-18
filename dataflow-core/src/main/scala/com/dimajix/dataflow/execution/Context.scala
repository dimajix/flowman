package com.dimajix.dataflow.execution

import org.slf4j.LoggerFactory

import com.dimajix.dataflow.spec.Database
import com.dimajix.dataflow.spec.DatabaseIdentifier
import com.dimajix.dataflow.spec.Profile
import com.dimajix.dataflow.spec.RelationIdentifier
import com.dimajix.dataflow.spec.model.Relation

case class SettingLevel(
    level:Int
)
object SettingLevel {
    val GLOBAL_OVERRIDE = new SettingLevel(300)
    val PROJECT_PROFILE = new SettingLevel(250)
    val PROJECT_SETTING = new SettingLevel(200)
    val NAMESPACE_PROFILE = new SettingLevel(150)
    val NAMESPACE_SETTING = new SettingLevel(100)
    val NONE = new SettingLevel(0)
}

abstract class Context {
    private val logger = LoggerFactory.getLogger(classOf[Executor])

    /**
      * Evaluates a string containing expressions to be processed.
      *
      * @param string
      * @return
      */
    def evaluate(string: String): String

    def getDatabase(name: DatabaseIdentifier): Database

    /**
      * Returns all configuration options as a key-value map
      *
      * @return
      */
    def config: Map[String, String]

    /**
      * Returns the current environment used for replacing variables
      *
      * @return
      */
    def environment: Map[String, String]

    def withEnvironment(env: Map[String, String]): Context
    def withEnvironment(env: Seq[(String, String)]): Context
    def withConfig(env:Map[String,String]) : Context
    def withConfig(env:Seq[(String,String)]) : Context
    def withProfile(profile:Profile) : Context
}
