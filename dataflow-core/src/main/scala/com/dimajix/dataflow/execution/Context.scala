package com.dimajix.dataflow.execution

import org.slf4j.LoggerFactory

import com.dimajix.dataflow.spec.Connection
import com.dimajix.dataflow.spec.ConnectionIdentifier
import com.dimajix.dataflow.spec.OutputIdentifier
import com.dimajix.dataflow.spec.Profile
import com.dimajix.dataflow.spec.RelationIdentifier
import com.dimajix.dataflow.spec.TableIdentifier
import com.dimajix.dataflow.spec.flow.Mapping
import com.dimajix.dataflow.spec.model.Relation
import com.dimajix.dataflow.spec.output.Output
import com.dimajix.dataflow.spec.runner.Runner

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

    /**
      * Try to retrieve the specified database connection. Performs lookups in parent context if required
      *
      * @param identifier
      * @return
      */
    def getConnection(identifier: ConnectionIdentifier): Connection
    /**
      * Returns a specific named MappingType. The Transform can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    def getMapping(identifier: TableIdentifier) : Mapping
    /**
      * Returns a specific named RelationType. The RelationType can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    def getRelation(identifier: RelationIdentifier): Relation
    /**
      * Returns a specific named OutputType. The OutputType can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    def getOutput(identifier: OutputIdentifier): Output

    /**
      * Returns the appropriate runner
      *
      * @return
      */
    def runner : Runner

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
