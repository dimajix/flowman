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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import com.dimajix.flowman.fs.File
import com.dimajix.flowman.namespace.Namespace
import com.dimajix.flowman.namespace.runner.Runner
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.OutputIdentifier
import com.dimajix.flowman.spec.Profile
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.connection.Connection
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.output.Output
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.util.Templating.FileWrapper


object ProjectContext {
    class Builder(_parent:Context, _project:Project) extends AbstractContext.Builder {
        override def withEnvironment(env: Seq[(String, Any)]): Builder = {
            withEnvironment(env, SettingLevel.PROJECT_SETTING)
            this
        }
        override def withConfig(config:Map[String,String]) : Builder = {
            withConfig(config, SettingLevel.PROJECT_SETTING)
            this
        }
        override def withConnections(connections:Map[String,Connection]) : Builder = {
            withConnections(connections, SettingLevel.PROJECT_SETTING)
            this
        }
        override def withProfile(profile:Profile) : Builder = {
            withProfile(profile, SettingLevel.PROJECT_PROFILE)
            this
        }

        override protected def createContext(): ProjectContext = {
            new ProjectContext(_parent, _project)
        }
    }

    def builder(parent:Context, project:Project) = new Builder(parent, project)
}


class ProjectContext(parent:Context, _project:Project) extends AbstractContext {
    override protected val logger = LoggerFactory.getLogger(classOf[ProjectContext])

    private object ProjectWrapper {
        def getBasedir() : FileWrapper = FileWrapper(_project.basedir)
        def getFilename() : FileWrapper = FileWrapper(_project.filename)
        def getName() : String = _project.name
        def getVersion() : String = _project.version
    }

    updateFrom(parent)
    templateContext.put("project", ProjectWrapper)

    /**
      * Returns the namespace associated with this context. Can be null
      * @return
      */
    override def namespace : Namespace = parent.namespace

    /**
      * Returns the project associated with this context. Can be null
      * @return
      */
    override def project : Project = _project

    /**
      * Returns the root context in a hierarchy of connected contexts
      * @return
      */
    override def root : Context = parent.root

    /**
      * Returns the appropriate runner for this project.
      *
      * @return
      */
    override def runner : Runner = {
        parent.runner
    }

    /**
      * Returns a specific named Transform. The Transform can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    override def getMapping(identifier: MappingIdentifier): Mapping = {
        if (identifier.project.forall(_ == _project.name))
            _project.mappings.getOrElse(identifier.name, throw new NoSuchElementException(s"Mapping '$identifier' not found in project ${_project.name}"))
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
            _project.relations.getOrElse(identifier.name, throw new NoSuchElementException(s"Relation '$identifier' not found in project ${_project.name}"))
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
            _project.outputs.getOrElse(identifier.name, throw new NoSuchElementException(s"Output '$identifier' not found in project ${_project.name}"))
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
        if (identifier.project.isEmpty) {
            databases.getOrElse(identifier.name, _project.connections.getOrElse(identifier.name, throw new NoSuchElementException(s"Connection '$identifier' not found in project ${_project.name}")))
        }
        else if (identifier.project.contains(_project.name)) {
            _project.connections.getOrElse(identifier.name, throw new NoSuchElementException(s"Connection '$identifier' not found in project ${_project.name}"))
        }
        else {
            parent.getConnection(identifier)
        }
    }

    /**
      * Returns a specific named Job. The JobType can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    override def getJob(identifier: JobIdentifier): Job = {
        if (identifier.project.forall(_ == _project.name))
            _project.jobs.getOrElse(identifier.name, throw new NoSuchElementException(s"Job $identifier not found in project ${_project.name}"))
        else
            parent.getJob(identifier)
    }

    override def getProjectContext(projectName:String) : Context = {
        parent.getProjectContext(projectName)
    }
    override def getProjectContext(project:Project) : Context = {
        parent.getProjectContext(project)
    }
}
