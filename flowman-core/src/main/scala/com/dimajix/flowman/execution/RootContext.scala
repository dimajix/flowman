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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.slf4j.LoggerFactory

import com.dimajix.flowman.fs.FileSystem
import com.dimajix.flowman.namespace.Namespace
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.OutputIdentifier
import com.dimajix.flowman.spec.Profile
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.connection.Connection
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.output.Output
import com.dimajix.flowman.spec.task.Job


object RootContext {
    class Builder(_namespace:Namespace, _profiles:Seq[String], _parent:Context = null) extends AbstractContext.Builder {
        private var _runner:Runner = new SimpleRunner()

        override def withEnvironment(env: Seq[(String, Any)]): Builder = {
            withEnvironment(env, SettingLevel.NAMESPACE_SETTING)
            this
        }
        override def withConfig(config:Map[String,String]) : Builder = {
            withConfig(config, SettingLevel.NAMESPACE_SETTING)
            this
        }
        override def withConnections(connections:Map[String,Connection]) : Builder = {
            withConnections(connections, SettingLevel.NAMESPACE_SETTING)
            this
        }
        override def withProfile(profile:Profile) : Builder = {
            withProfile(profile, SettingLevel.NAMESPACE_PROFILE)
            this
        }
        def withRunner(runner:Runner) : Builder = {
            _runner = runner
            this
        }

        override def createContext(): RootContext = {
            val context = new RootContext(_runner, _namespace, _profiles)
            if (_parent != null)
                context.updateFrom(_parent)
            context
        }
    }

    def builder() = new Builder(null, Seq())
    def builder(namespace:Namespace, profiles:Seq[String]) = new Builder(namespace, profiles)
    def builder(parent:Context) = new Builder(parent.namespace, Seq(), parent)
}


class RootContext private[execution](_runner:Runner, _namespace:Namespace, _profiles:Seq[String]) extends AbstractContext {
    override protected val logger = LoggerFactory.getLogger(classOf[RootContext])
    private val _children: mutable.Map[String, Context] = mutable.Map()
    private lazy val _sparkConf = new SparkConf().setAll(config.toSeq)
    private lazy val _hadoopConf = SparkHadoopUtil.get.newConfiguration(_sparkConf)
    private lazy val _fs = new FileSystem(_hadoopConf)


    def profiles : Seq[String] = _profiles

    /**
      * Returns the namespace associated with this context. Can be null
      * @return
      */
    override def namespace : Namespace = _namespace

    /**
      * Returns the project associated with this context. Can be null
      * @return
      */
    override def project: Project = null

    /**
      * Returns the root context in a hierarchy of connected contexts
      * @return
      */
    override def root : Context = this

    /**
      * Returns the appropriate runner
      *
      * @return
      */
    override def runner : Runner = _runner

    /**
      * Returns a fully qualified mapping from a project belonging to the namespace of this executor
      *
      * @param identifier
      * @return
      */
    override def getMapping(identifier: MappingIdentifier): Mapping = {
        if (identifier.project.isEmpty)
            throw new NoSuchElementException(s"Cannot find mapping with name '$identifier'")
        val child = getProjectContext(identifier.project.get)
        child.getMapping(MappingIdentifier(identifier.name, None))
    }
    /**
      * Returns a fully qualified relation from a project belonging to the namespace of this executor
      *
      * @param identifier
      * @return
      */
    override def getRelation(identifier: RelationIdentifier): Relation = {
        if (identifier.project.isEmpty)
            throw new NoSuchElementException(s"Cannot find relation with name '$identifier'")
        val child = getProjectContext(identifier.project.get)
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
            throw new NoSuchElementException(s"Cannot find output with name '$identifier'")
        val child = getProjectContext(identifier.project.get)
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
            con.getOrElse(throw new NoSuchElementException(s"Cannot find connection with name '$identifier'"))
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
            throw new NoSuchElementException(s"Cannot find Job with name '$identifier'")
        val child = getProjectContext(identifier.project.get)
        child.getJob(JobIdentifier(identifier.name, None))
    }

    /**
      * Returns the context for a specific project
      *
      * @param projectName
      * @return
      */
    override def getProjectContext(projectName:String) : Context = {
        _children.getOrElseUpdate(projectName, createProjectContext(loadProject(projectName)))
    }
    override def getProjectContext(project:Project) : Context = {
        _children.getOrElseUpdate(project.name, createProjectContext(project))
    }

    private def createProjectContext(project: Project) : Context = {
        val builder = ProjectContext.builder(this, project)
        profiles.foreach { prof =>
                project.profiles.get(prof).foreach { profile =>
                    logger.info(s"Applying project profile $prof")
                    builder.withProfile(profile)
                }
            }

        val context = builder.withEnvironment(project.environment)
            .withConfig(project.config.toMap)
            .build()

        _children.update(project.name, context)
        context
    }
    private def loadProject(name: String): Project = {
        _namespace.store.loadProject(name)
    }

    override def fs : FileSystem = _fs

    override def sparkConf: SparkConf = _sparkConf

    override def hadoopConf: Configuration = _hadoopConf
}
