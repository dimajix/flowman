/*
 * Copyright 2018 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.execution

import java.util.NoSuchElementException

import scala.collection.mutable

import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.Connection
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.OutputIdentifier
import com.dimajix.flowman.spec.Profile
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.TableIdentifier
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.output.Output
import com.dimajix.flowman.spec.runner.Runner
import com.dimajix.flowman.spec.runner.SimpleRunner
import com.dimajix.flowman.spec.task.Job



class RootContext private[execution](_namespace:Namespace, _profiles:Seq[String]) extends AbstractContext {
    override protected val logger = LoggerFactory.getLogger(classOf[RootContext])
    private val _children: mutable.Map[String, Context] = mutable.Map()
    private val _runner = new SimpleRunner()

    def profiles : Seq[String] = _profiles

    override def namespace : Namespace = _namespace

    override def project: Project = null

    override def root : Context = this

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
      * @param identifier
      * @return
      */
    override def getMapping(identifier: TableIdentifier): Mapping = {
        if (identifier.project.isEmpty)
            throw new NoSuchElementException(s"Expected project name in mapping specifier '$identifier'")
        val child = _children.getOrElseUpdate(identifier.project.get, getProjectContext(identifier.project.get))
        child.getMapping(TableIdentifier(identifier.name, None))
    }
    /**
      * Returns a fully qualified relation from a project belonging to the namespace of this executor
      *
      * @param identifier
      * @return
      */
    override def getRelation(identifier: RelationIdentifier): Relation = {
        if (identifier.project.isEmpty)
            throw new NoSuchElementException(s"Expected project name in relation specifier '$identifier'")
        val child = _children.getOrElseUpdate(identifier.project.get, getProjectContext(identifier.project.get))
        child.getRelation(RelationIdentifier(identifier.name, None))
    }

    /**
      * Returns a fully qualified output from a project belonging to the namespace of this executor
      *
      * @param identifier
      * @return
      */
    override def getOutput(identifier: OutputIdentifier): Output = {
        if (identifier.project.isEmpty)
            throw new NoSuchElementException(s"Expected project name in output specifier '$identifier'")
        val child = _children.getOrElseUpdate(identifier.project.get, getProjectContext(identifier.project.get))
        child.getOutput(OutputIdentifier(identifier.name, None))
    }

    /**
      * Returns a fully qualified connection from a project belonging to the namespace of this executor
      *
      * @param identifier
      * @return
      */
    override def getConnection(identifier:ConnectionIdentifier) : Connection = {
        if (identifier.project.isEmpty) {
            val con = Option(namespace).flatMap(_.connections.get(identifier.name))
            con.getOrElse(throw new NoSuchElementException(s"Expected project name in connection specifier '$identifier'"))
        }
        else {
            val child = getProjectContext(identifier.project.get)
            child.getConnection(ConnectionIdentifier(identifier.name, None))
        }
    }

    /**
      * Returns a fully qualified job from a project belonging to the namespace of this executor
      *
      * @param identifier
      * @return
      */
    override def getJob(identifier: JobIdentifier): Job = {
        if (identifier.project.isEmpty)
            throw new NoSuchElementException(s"Expected project name in Job specifier '$identifier'")
        val child = _children.getOrElseUpdate(identifier.project.get, getProjectContext(identifier.project.get))
        child.getJob(JobIdentifier(identifier.name, None))
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
        setConnections(profile.connections, SettingLevel.NAMESPACE_PROFILE)
        this
    }

    /**
      * Returns the context for a specific project
      *
      * @param name
      * @return
      */
    def getProjectContext(name:String) : Context = {
        _children.getOrElseUpdate(name, createProjectContext(loadProject(name)))
    }
    def getProjectContext(project:Project) : Context = {
        _children.getOrElseUpdate(project.name, createProjectContext(project))
    }

    private def createProjectContext(project: Project) : Context = {
        profiles.foldLeft(newProjectContext(project)) { (context,prof) =>
                project.profiles.get(prof).foreach { profile =>
                    logger.info(s"Applying project profile $prof")
                    context.withProfile(profile)
                }
                context
            }
            .withEnvironment(project.environment)
            .withConfig(project.config)
    }
    def loadProject(name: String): Project = {
        val project : Project = ???
        project
    }
}
