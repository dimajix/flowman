package com.dimajix.flowman.execution

import com.dimajix.flowman.spec.Connection
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.OutputIdentifier
import com.dimajix.flowman.spec.Profile
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.TableIdentifier
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.output.Output
import com.dimajix.flowman.spec.runner.Runner


class ProjectContext(parent:RootContext, _project:Project) extends AbstractContext {
    updateFrom(parent)

    def project : Project = _project

    /**
      * Returns the appropriate runner for this project.
      *
      * @return
      */
    override def runner : Runner = {
        if (_project.runner != null)
            _project.runner
        else
            parent.runner
    }

    /**
      * Returns a specific named Transform. The Transform can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    override def getMapping(identifier: TableIdentifier): Mapping = {
        if (identifier.project.forall(_ == _project.name))
            _project.mappings.getOrElse(identifier.name, throw new NoSuchElementException(s"MappingType $identifier not found in project ${_project.name}"))
        else
            parent.getMapping(identifier)
    }

    /**
      * Returns a specific named RelationType. The RelationType can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    override def getRelation(identifier: RelationIdentifier): Relation = {
        if (identifier.project.forall(_ == _project.name))
            _project.relations.getOrElse(identifier.name, throw new NoSuchElementException(s"RelationType $identifier not found in project ${_project.name}"))
        else
            parent.getRelation(identifier)
    }

    /**
      * Returns a specific named OutputType. The OutputType can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    override def getOutput(identifier: OutputIdentifier): Output = {
        if (identifier.project.forall(_ == _project.name))
            _project.outputs.getOrElse(identifier.name, throw new NoSuchElementException(s"OutputType $identifier not found in project ${_project.name}"))
        else
            parent.getOutput(identifier)
    }

    /**
      * Try to retrieve the specified database. Performs lookups in parent context if required
      *
      * @param identifier
      * @return
      */
    override def getConnection(identifier:ConnectionIdentifier) : Connection = {
        if (identifier.project.forall(_ == _project.name)) {
            databases.getOrElse(identifier.name, throw new NoSuchElementException(s"Database $identifier not found in project ${_project.name}"))
        }
        else {
            parent.getConnection(identifier)
        }
    }

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
