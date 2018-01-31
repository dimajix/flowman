package com.dimajix.dataflow.execution

import java.util.NoSuchElementException

import scala.collection.mutable

import org.slf4j.LoggerFactory

import com.dimajix.dataflow.spec.Connection
import com.dimajix.dataflow.spec.ConnectionIdentifier
import com.dimajix.dataflow.spec.Namespace
import com.dimajix.dataflow.spec.OutputIdentifier
import com.dimajix.dataflow.spec.Profile
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.spec.RelationIdentifier
import com.dimajix.dataflow.spec.TableIdentifier
import com.dimajix.dataflow.spec.flow.Mapping
import com.dimajix.dataflow.spec.model.Relation
import com.dimajix.dataflow.spec.output.Output
import com.dimajix.dataflow.spec.runner.Runner
import com.dimajix.dataflow.spec.runner.SimpleRunner



class RootContext private[execution](_namespace:Namespace, _profiles:Seq[String]) extends AbstractContext {
    private val logger = LoggerFactory.getLogger(classOf[RootContext])
    private val _children: mutable.Map[String, ProjectContext] = mutable.Map()
    private val _runner = new SimpleRunner()

    def namespace : Namespace = _namespace
    def profiles : Seq[String] = _profiles

    /**
      * Returns the appropriate runner
      *
      * @return
      */
    override def runner : Runner = {
        if (_namespace != null && _namespace.runner != null)
            _namespace.runner
        else
            _runner
    }

    /**
      * Returns a fully qualified mapping from a project belonging to the namespace of this executor
      *
      * @param name
      * @return
      */
    override def getMapping(name: TableIdentifier): Mapping = {
        if (name.project.isEmpty)
            throw new NoSuchElementException("Expected project name in mapping specifier")
        val child = _children.getOrElseUpdate(name.project.get, getProjectContext(name.project.get))
        child.getMapping(TableIdentifier(name.name, None))
    }
    /**
      * Returns a fully qualified relation from a project belonging to the namespace of this executor
      *
      * @param name
      * @return
      */
    override def getRelation(name: RelationIdentifier): Relation = {
        if (name.project.isEmpty)
            throw new NoSuchElementException("Expected project name in relation specifier")
        val child = _children.getOrElseUpdate(name.project.get, getProjectContext(name.project.get))
        child.getRelation(RelationIdentifier(name.name, None))
    }
    /**
      * Returns a fully qualified output from a project belonging to the namespace of this executor
      *
      * @param name
      * @return
      */
    override def getOutput(name: OutputIdentifier): Output = {
        if (name.project.isEmpty)
            throw new NoSuchElementException("Expected project name in output specifier")
        val child = _children.getOrElseUpdate(name.project.get, getProjectContext(name.project.get))
        child.getOutput(OutputIdentifier(name.name, None))
    }

    override def getConnection(identifier:ConnectionIdentifier) : Connection = {
        if (identifier.project.isEmpty)
            throw new NoSuchElementException("Expected project name in table specifier")
        val child = getProjectContext(identifier.project.get)
        child.getConnection(ConnectionIdentifier(identifier.name, None))
    }

    /**
      * Creates a new derived Context for use with projects
      * @param project
      * @return
      */
    def newProjectContext(project:Project) : ProjectContext = {
        val result = new ProjectContext(this, project)
        _children.update(project.name, result)
        result
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
        setEnvironment(env, SettingLevel.NAMESPACE_SETTING)
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
        setConfig(env, SettingLevel.NAMESPACE_SETTING)
        this
    }

    /**
      * Creates a new chained context with additional properties from a profile
      * @param profile
      * @return
      */
    override def withProfile(profile:Profile) : Context = {
        setConfig(profile.config, SettingLevel.NAMESPACE_PROFILE)
        setEnvironment(profile.environment, SettingLevel.NAMESPACE_PROFILE)
        setDatabases(profile.databases, SettingLevel.NAMESPACE_PROFILE)
        this
    }

    /**
      * Returns the context for a specific project
      *
      * @param name
      * @return
      */
    def getProjectContext(name:String) : ProjectContext = {
        _children.getOrElseUpdate(name, createProjectContext(loadProject(name)))
    }
    def getProjectContext(project:Project) : ProjectContext = {
        _children.getOrElseUpdate(project.name, createProjectContext(project))
    }

    private def createProjectContext(project: Project) : ProjectContext = {
        val pcontext = newProjectContext(project)
        profiles.foreach(p => project.profiles.get(p).foreach { profile =>
            logger.info(s"Applying project profile $p")
            pcontext.withProfile(profile)
        })
        pcontext.withEnvironment(project.environment)
        pcontext.withConfig(project.config)
        pcontext
    }
    private def loadProject(name: String): Project = {
        val project : Project = ???
        project
    }
}
