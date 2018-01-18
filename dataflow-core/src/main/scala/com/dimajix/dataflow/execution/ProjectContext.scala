package com.dimajix.dataflow.execution

import com.dimajix.dataflow.spec.Profile
import com.dimajix.dataflow.spec.Project


class ProjectContext(parent:RootContext, _project:Project) extends AbstractContext {
    updateFrom(parent)

    def project : Project = _project

    /**
      * Creates a new chained context with additional environment variables
      * @param env
      * @return
      */
    override def withEnvironment(env:Map[String,String]) : Context = {
        withEnvironment(env.toSeq)
    }
    override def withEnvironment(env:Seq[(String,String)]) : Context = {
        setEnvironment(env, SettingLevel.PROJECT_SETTING)
        this
    }

    /**
      * Creates a new chained context with additional Spark configuration variables
      * @param env
      * @return
      */
    override def withConfig(env:Map[String,String]) : Context = {
        withConfig(env.toSeq)
    }
    override def withConfig(env:Seq[(String,String)]) : Context = {
        setConfig(env, SettingLevel.PROJECT_SETTING)
        this
    }

    /**
      * Creates a new chained context with additional properties from a profile
      * @param profile
      * @return
      */
    override def withProfile(profile:Profile) : Context = {
        setConfig(profile.config, SettingLevel.PROJECT_PROFILE)
        setEnvironment(profile.environment, SettingLevel.PROJECT_PROFILE)
        setDatabases(profile.databases, SettingLevel.PROJECT_PROFILE)
        this
    }
}
