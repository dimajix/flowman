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

import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Profile
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.spec.connection.Connection
import com.dimajix.flowman.spec.connection.ConnectionSpec
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.target.Target
import com.dimajix.flowman.spec.task.Job


object RootContext {
    class Builder private[RootContext](namespace:Namespace, profiles:Seq[String], _parent:Context = null) extends AbstractContext.Builder(_parent, SettingLevel.NAMESPACE_SETTING) {
        override protected val logger = LoggerFactory.getLogger(classOf[RootContext])

        override def withProfile(profile:Profile) : Builder = {
            withProfile(profile, SettingLevel.NAMESPACE_PROFILE)
            this
        }

        override protected def createContext(env:Map[String,(Any, Int)], config:Map[String,(String, Int)], connections:Map[String, ConnectionSpec]) : RootContext = {
            case object NamespaceWrapper {
                def getName() : String = namespace.name
                override def toString: String = namespace.name
            }

            val fullEnv = if (namespace != null)
                env + ("namespace" -> ((NamespaceWrapper, SettingLevel.SCOPE_OVERRIDE.level)))
            else
                env

            new RootContext(namespace, profiles, fullEnv, config, connections)
        }
    }

    def builder() = new Builder(null, Seq())
    def builder(namespace:Namespace, profiles:Seq[String]) = new Builder(namespace, profiles)
    def builder(parent:Context) = new Builder(parent.namespace, Seq(), parent)
}


class RootContext private[execution](
        _namespace:Namespace,
        _profiles:Seq[String],
        fullEnv:Map[String,(Any, Int)],
        fullConfig:Map[String,(String, Int)],
        nonNamespaceConnections:Map[String, ConnectionSpec]
) extends AbstractContext(null, fullEnv, fullConfig) {
    private val _children: mutable.Map[String, Context] = mutable.Map()
    private lazy val _sparkConf = new SparkConf().setAll(config.toSeq)
    private lazy val _hadoopConf = SparkHadoopUtil.get.newConfiguration(_sparkConf)
    private lazy val _fs = FileSystem(_hadoopConf)

    private val connections = mutable.Map[String,Connection]()


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
      * Returns a fully qualified mapping from a project belonging to the namespace of this executor
      *
      * @param identifier
      * @return
      */
    override def getMapping(identifier: MappingIdentifier): Mapping = {
        require(identifier != null && identifier.nonEmpty)

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
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.isEmpty)
            throw new NoSuchElementException(s"Cannot find relation with name '$identifier'")
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
            throw new NoSuchElementException(s"Cannot find output with name '$identifier'")
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
                        Option(namespace)
                            .flatMap(_.connections.get(identifier.name))
                    )
                    .getOrElse(throw new NoSuchElementException(s"Cannot find connection with name '$identifier'"))
                    .instantiate(this)
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
        require(projectName != null && projectName.nonEmpty)
        _children.getOrElseUpdate(projectName, createProjectContext(loadProject(projectName)))
    }
    override def getProjectContext(project:Project) : Context = {
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
        _namespace.store.loadProject(name)
    }

    /**
      * Returns the FileSystem as configured in Hadoop
      * @return
      */
    override def fs : FileSystem = _fs

    /**
      * Returns a SparkConf object, which contains all Spark settings as specified in the conifguration. The object
      * is not necessarily the one used by the Spark Session!
      * @return
      */
    override def sparkConf: SparkConf = _sparkConf

    /**
      * Returns a Hadoop Configuration object which contains all settings form the configuration. The object is not
      * necessarily the one used by the active Spark session
      * @return
      */
    override def hadoopConf: Configuration = _hadoopConf
}
