package com.dimajix.dataflow.execution

import scala.collection.mutable

import com.dimajix.dataflow.spec.Namespace
import com.dimajix.dataflow.spec.Profile
import com.dimajix.dataflow.spec.Project



class RootContext private[execution](_namespace:Namespace) extends AbstractContext {
    private val _children: mutable.Map[String, Context] = mutable.Map()

    def namespace : Namespace = _namespace

    /**
      * Creates a new derived Context for use with projects
      * @param project
      * @return
      */
    def newProjectContext(project:Project) : Context = {
        val result = new ProjectContext(this, project)
        _children.update(project.name, result)
        result
    }

    /**
      * Creates a new chained context with additional environment variables
      * @param env
      * @return
      */
    def withEnvironment(env:Map[String,String]) : Context = {
        withEnvironment(env.toSeq)
    }
    def withEnvironment(env:Seq[(String,String)]) : Context = {
        setEnvironment(env, SettingLevel.NAMESPACE_SETTING)
        this
    }

    /**
      * Creates a new chained context with additional Spark configuration variables
      * @param env
      * @return
      */
    def withConfig(env:Map[String,String]) : Context = {
        withConfig(env.toSeq)
    }
    def withConfig(env:Seq[(String,String)]) : Context = {
        setConfig(env, SettingLevel.NAMESPACE_SETTING)
        this
    }

    /**
      * Creates a new chained context with additional properties from a profile
      * @param profile
      * @return
      */
    def withProfile(profile:Profile) : Context = {
        setConfig(profile.config, SettingLevel.NAMESPACE_PROFILE)
        setEnvironment(profile.environment, SettingLevel.NAMESPACE_PROFILE)
        setDatabases(profile.databases, SettingLevel.NAMESPACE_PROFILE)
        this
    }
}
