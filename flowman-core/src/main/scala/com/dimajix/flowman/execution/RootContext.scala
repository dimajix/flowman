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

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import com.dimajix.flowman.config.Configuration
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.execution.ProjectContext.Builder
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.NamespaceWrapper
import com.dimajix.flowman.model.Profile
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestIdentifier


object RootContext {
    class Builder private[RootContext](namespace:Option[Namespace], profiles:Set[String], parent:Context = null) extends AbstractContext.Builder[Builder,RootContext](parent, SettingLevel.NAMESPACE_SETTING) {
        private var projectResolver:Option[String => Option[Project]] = None
        private var overrideMappings:Map[MappingIdentifier, Prototype[Mapping]] = Map()
        private var overrideRelations:Map[RelationIdentifier, Prototype[Relation]] = Map()
        private var execution:Option[Execution] = None

        override protected val logger = LoggerFactory.getLogger(classOf[RootContext])

        override def withProfile(profile:Profile) : Builder = {
            withProfile(profile, SettingLevel.NAMESPACE_PROFILE)
            this
        }

        def withProjectResolver(resolver:String => Option[Project]) : Builder = {
            projectResolver = Some(resolver)
            this
        }

        def withExecution(execution:Execution) : Builder = {
            this.execution = Some(execution)
            this
        }
        def withExecution(execution:Option[Execution]) : Builder = {
            this.execution = execution
            this
        }

        /**
         * Add extra mappings, which potentially override existing project mappings
         * @param mappings
         * @return
         */
        def overrideMappings(mappings:Map[MappingIdentifier,Prototype[Mapping]]) : Builder = {
            if (mappings.keySet.exists(_.project.isEmpty))
                throw new IllegalArgumentException("MappingIdentifiers need to contain valid project for overriding")
            overrideMappings = overrideMappings ++ mappings
            this
        }

        /**
         * Adds extra relations, which potentially override existing project relations
         * @param relations
         * @return
         */
        def overrideRelations(relations:Map[RelationIdentifier,Prototype[Relation]]) : Builder = {
            if (relations.keySet.exists(_.project.isEmpty))
                throw new IllegalArgumentException("RelationIdentifiers need to contain valid project for overriding")
            overrideRelations = overrideRelations ++ relations
            this
        }

        override protected def createContext(env:Map[String,(Any, Int)], config:Map[String,(String, Int)], connections:Map[String, Prototype[Connection]]) : RootContext = {
            new RootContext(namespace, projectResolver, profiles, env, config, execution, connections, overrideMappings, overrideRelations)
        }
    }

    def builder() = new Builder(None, Set())
    def builder(namespace:Option[Namespace], profiles:Set[String]) = new Builder(namespace, profiles)
    def builder(parent:Context) = new Builder(parent.namespace, Set(), parent)
}


final class RootContext private[execution](
    _namespace:Option[Namespace],
    projectResolver:Option[String => Option[Project]],
    _profiles:Set[String],
    _env:Map[String,(Any, Int)],
    _config:Map[String,(String, Int)],
    _execution:Option[Execution],
    extraConnections:Map[String, Prototype[Connection]],
    overrideMappings:Map[MappingIdentifier, Prototype[Mapping]],
    overrideRelations:Map[RelationIdentifier, Prototype[Relation]]
) extends AbstractContext(
    _env + ("namespace" -> (NamespaceWrapper(_namespace) -> SettingLevel.SCOPE_OVERRIDE.level)),
    _config
) {
    private val _children: mutable.Map[String, Context] = mutable.Map()
    private lazy val _fs = FileSystem(hadoopConf)
    private lazy val _exec = _execution match {
        case Some(execution) => execution
        case None => new AnalyzingExecution(this)
    }

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
      * Returns the root context in a hierarchy of connected contexts. In the case of a [[RootContext]], the
      * context itself is returned.
      * @return
      */
    override def root : RootContext = this

    /**
     * Returns the list of active profile names
     *
     * @return
     */
    override def profiles: Set[String] = _profiles

    /**
      * Returns a fully qualified mapping from a project belonging to the namespace of this execution
      *
      * @param identifier
      * @return
      */
    override def getMapping(identifier: MappingIdentifier, allowOverrides:Boolean=true): Mapping = {
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.isEmpty)
            throw new NoSuchMappingException(identifier)
        val child = getProjectContext(identifier.project.get)
        child.getMapping(identifier, allowOverrides)
    }

    /**
      * Returns a fully qualified relation from a project belonging to the namespace of this execution
      *
      * @param identifier
      * @return
      */
    override def getRelation(identifier: RelationIdentifier, allowOverrides:Boolean=true): Relation = {
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.isEmpty)
            throw new NoSuchRelationException(identifier)
        val child = getProjectContext(identifier.project.get)
        child.getRelation(identifier, allowOverrides)
    }

    /**
      * Returns a fully qualified target from a project belonging to the namespace of this execution
      *
      * @param identifier
      * @return
      */
    override def getTarget(identifier: TargetIdentifier): Target = {
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.isEmpty)
            throw new NoSuchTargetException(identifier)
        val child = getProjectContext(identifier.project.get)
        child.getTarget(identifier)
    }

    /**
      * Returns a fully qualified connection from a project belonging to the namespace of this execution
      *
      * @param identifier
      * @return
      */
    override def getConnection(identifier:ConnectionIdentifier) : Connection = {
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.isEmpty) {
            connections.getOrElseUpdate(identifier.name,
                extraConnections.get(identifier.name)
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
            child.getConnection(identifier)
        }
    }

    /**
      * Returns a fully qualified job from a project belonging to the namespace of this execution
      *
      * @param identifier
      * @return
      */
    override def getJob(identifier: JobIdentifier): Job = {
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.isEmpty)
            throw new NoSuchJobException(identifier)
        val child = getProjectContext(identifier.project.get)
        child.getJob(identifier)
    }

    /**
     * Returns a fully qualified test from a project belonging to the namespace of this execution
     *
     * @param identifier
     * @return
     */
    override def getTest(identifier: TestIdentifier): Test = {
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.isEmpty)
            throw new NoSuchTestException(identifier)
        val child = getProjectContext(identifier.project.get)
        child.getTest(identifier)
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

        // Apply overrides
        builder.overrideMappings(overrideMappings.filter(_._1.project.contains(project.name)).map(kv => (kv._1.name, kv._2)))
        builder.overrideRelations(overrideRelations.filter(_._1.project.contains(project.name)).map(kv => (kv._1.name, kv._2)))

        val context = builder.withEnvironment(project.environment)
            .withConfig(project.config)
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
    override def flowmanConf : FlowmanConf = config.flowmanConf

    /**
      * Returns a SparkConf object, which contains all Spark settings as specified in the conifguration. The object
      * is not necessarily the one used by the Spark Session!
      * @return
      */
    override def sparkConf: SparkConf = config.sparkConf

    /**
      * Returns a Hadoop Configuration object which contains all settings form the configuration. The object is not
      * necessarily the one used by the active Spark session
      * @return
      */
    override def hadoopConf: HadoopConf = config.hadoopConf

    /**
     * Returns a possibly shared execution environment. The execution can be a [[AnalyzingExecution]] with limited
     * capabilities, so you should always prefer to employ
     *
     * @return
     */
    override def execution: Execution = _exec
}
