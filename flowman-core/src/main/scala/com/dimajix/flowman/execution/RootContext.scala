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

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Profile
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.Template


object RootContext {
    class Builder private[RootContext](namespace:Option[Namespace], profiles:Seq[String], parent:Context = null) extends AbstractContext.Builder[Builder,RootContext](parent, SettingLevel.NAMESPACE_SETTING) {
        private var projectResolver:Option[String => Option[Project]] = None

        override protected val logger = LoggerFactory.getLogger(classOf[RootContext])

        override def withProfile(profile:Profile) : Builder = {
            withProfile(profile, SettingLevel.NAMESPACE_PROFILE)
            this
        }

        def withProjectResolver(resolver:String => Option[Project]) : Builder = {
            projectResolver = Some(resolver)
            this
        }

        override protected def createContext(env:Map[String,(Any, Int)], config:Map[String,(String, Int)], connections:Map[String, Template[Connection]]) : RootContext = {
            class NamespaceWrapper(namespace:Namespace) {
                def getName() : String = namespace.name
                override def toString: String = namespace.name
            }

            val fullEnv = env ++
                namespace.map(ns => "namespace" -> (new NamespaceWrapper(ns) -> SettingLevel.SCOPE_OVERRIDE.level)).toMap

            new RootContext(namespace, projectResolver, profiles, fullEnv, config, connections)
        }
    }

    def builder() = new Builder(None, Seq())
    def builder(namespace:Option[Namespace], profiles:Seq[String]) = new Builder(namespace, profiles)
    def builder(parent:Context) = new Builder(parent.namespace, Seq(), parent)
}


class RootContext private[execution](
    _namespace:Option[Namespace],
    projectResolver:Option[String => Option[Project]],
    profiles:Seq[String],
    fullEnv:Map[String,(Any, Int)],
    fullConfig:Map[String,(String, Int)],
    nonNamespaceConnections:Map[String, Template[Connection]]
) extends AbstractContext(null, fullEnv, fullConfig) {
    private val _children: mutable.Map[String, Context] = mutable.Map()
    private val _configuration = new com.dimajix.flowman.config.Configuration(config)
    private lazy val _fs = FileSystem(hadoopConf)

    private val connections = mutable.Map[String,Connection]()

    /**
      * Returns the namespace associated with this context. Can be null
      * @return
      */
    override def namespace : Option[Namespace] = _namespace

    /**
      * Returns the project associated with this context. Can be null
      * @return
      */
    override def project: Option[Project] = None

    /**
      * Returns the root context in a hierarchy of connected contexts
      * @return
      */
    override def root : RootContext = this

    /**
      * Returns a fully qualified mapping from a project belonging to the namespace of this executor
      *
      * @param identifier
      * @return
      */
    override def getMapping(identifier: MappingIdentifier): Mapping = {
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.isEmpty)
            throw new NoSuchMappingException(identifier)
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
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.isEmpty)
            throw new NoSuchRelationException(identifier)
        val child = getProjectContext(identifier.project.get)
        child.getRelation(RelationIdentifier(identifier.name, None))
    }

    /**
      * Returns a fully qualified target from a project belonging to the namespace of this executor
      *
      * @param identifier
      * @return
      */
    override def getTarget(identifier: TargetIdentifier): Target = {
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.isEmpty)
            throw new NoSuchTargetException(identifier)
        val child = getProjectContext(identifier.project.get)
        child.getTarget(TargetIdentifier(identifier.name, None))
    }

    /**
      * Returns a fully qualified connection from a project belonging to the namespace of this executor
      *
      * @param identifier
      * @return
      */
    override def getConnection(identifier:ConnectionIdentifier) : Connection = {
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.isEmpty) {
            connections.getOrElseUpdate(identifier.name,
                nonNamespaceConnections.get(identifier.name)
                    .orElse(
                        namespace
                            .flatMap(_.connections.get(identifier.name))
                    )
                    .map(_.instantiate(this))
                    .getOrElse(throw new NoSuchConnectionException(identifier))
            )
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
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.isEmpty)
            throw new NoSuchJobException(identifier)
        val child = getProjectContext(identifier.project.get)
        child.getJob(JobIdentifier(identifier.name, None))
    }

    /**
      * Returns the context for a specific project
      *
      * @param projectName
      * @return
      */
    private def getProjectContext(projectName:String) : Context = {
        require(projectName != null && projectName.nonEmpty)
        _children.getOrElseUpdate(projectName, createProjectContext(loadProject(projectName)))
    }
    def getProjectContext(project:Project) : Context = {
        require(project != null)
        _children.getOrElseUpdate(project.name, createProjectContext(project))
    }

    private def createProjectContext(project: Project) : Context = {
        val builder = ProjectContext.builder(this, project)
        profiles.foreach { prof =>
                project.profiles.get(prof).foreach { profile =>
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
        projectResolver.flatMap(f => f(name)).getOrElse(throw new NoSuchProjectException(name))
    }

    /**
      * Returns the FileSystem as configured in Hadoop
      * @return
      */
    override def fs : FileSystem = _fs

    /**
     * Returns the FlowmanConf object, which contains all Flowman settings.
     * @return
     */
    override def flowmanConf : FlowmanConf = _configuration.flowmanConf

    /**
      * Returns a SparkConf object, which contains all Spark settings as specified in the conifguration. The object
      * is not necessarily the one used by the Spark Session!
      * @return
      */
    override def sparkConf: SparkConf = _configuration.sparkConf

    /**
      * Returns a Hadoop Configuration object which contains all settings form the configuration. The object is not
      * necessarily the one used by the active Spark session
      * @return
      */
    override def hadoopConf: Configuration = _configuration.hadoopConf
}
