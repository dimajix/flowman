/*
 * Copyright 2018-2023 Kaya Kupferschmidt
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

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.fs.FileSystem
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
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestIdentifier


object RootContext {
    class Builder private[RootContext](namespace:Option[Namespace], profiles:Set[String]=Set.empty, parent:Option[Context]=None) extends AbstractContext.Builder[Builder,RootContext](parent, SettingLevel.NAMESPACE_SETTING) {
        private var projects:Seq[Project] = parent.map(_.root).toSeq.flatMap(_.projects)
        private var overrideMappings:Map[MappingIdentifier, Prototype[Mapping]] = Map()
        private var overrideRelations:Map[RelationIdentifier, Prototype[Relation]] = Map()
        private var execution:Option[Execution] = None

        override protected val logger = LoggerFactory.getLogger(classOf[RootContext])

        override def withProfile(profile:Profile) : Builder = {
            withProfile(profile, SettingLevel.NAMESPACE_PROFILE)
            this
        }

        def withProjects(projects:Seq[Project]) : Builder = {
            this.projects = projects.toList
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
            new RootContext(namespace, projects, profiles, env, config, execution, connections, overrideMappings, overrideRelations)
        }
    }

    def builder() : Builder = new Builder(None)
    def builder(namespace:Namespace) : Builder = new Builder(Some(namespace))
    def builder(namespace:Option[Namespace], profiles:Set[String]) : Builder = new Builder(namespace, profiles)
    def builder(parent:Context) : Builder = new Builder(parent.namespace, parent.root.profiles, Some(parent))
        .withConnections(parent.root.extraConnections)
        //.overrideRelations(parent.root.overrideRelations)
        //.overrideMappings(parent.root.overrideMappings)
}


final class RootContext private[execution](
    _namespace:Option[Namespace],
    private val projects:Seq[Project],
    private val profiles:Set[String],
    _env:Map[String,(Any, Int)],
    _config:Map[String,(String, Int)],
    _execution:Option[Execution],
    private val extraConnections:Map[String, Prototype[Connection]],
    private val overrideMappings:Map[MappingIdentifier, Prototype[Mapping]],
    private val overrideRelations:Map[RelationIdentifier, Prototype[Relation]]
) extends AbstractContext(
    _env + ("namespace" -> (NamespaceWrapper(_namespace) -> SettingLevel.SCOPE_OVERRIDE.level)),
    _config
) {
    private val _children: TrieMap[String, Context] = TrieMap()
    private val _imports:TrieMap[String,(Context,Project.Import)] = TrieMap()
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
      * Returns a fully qualified mapping from a project belonging to the namespace of this execution
      *
      * @param identifier
      * @return
      */
    @throws[InstantiateMappingFailedException]
    @throws[NoSuchMappingException]
    @throws[UnknownProjectException]
    override def getMapping(identifier: MappingIdentifier, allowOverrides:Boolean=true): Mapping = {
        require(identifier != null && identifier.nonEmpty)

        identifier.project match {
            case None => throw new NoSuchMappingException(identifier)
            case Some(project) =>
                val child = getProjectContext(project)
                child.getMapping(identifier, allowOverrides)
        }
    }

    /**
      * Returns a fully qualified relation from a project belonging to the namespace of this execution
      *
      * @param identifier
      * @return
      */
    @throws[InstantiateRelationFailedException]
    @throws[NoSuchRelationException]
    @throws[UnknownProjectException]
    override def getRelation(identifier: RelationIdentifier, allowOverrides:Boolean=true): Relation = {
        require(identifier != null && identifier.nonEmpty)

        identifier.project match {
            case None => throw new NoSuchRelationException (identifier)
            case Some(project) =>
                val child = getProjectContext (project)
                child.getRelation (identifier, allowOverrides)
        }
    }

    /**
      * Returns a fully qualified target from a project belonging to the namespace of this execution
      *
      * @param identifier
      * @return
      */
    @throws[InstantiateTargetFailedException]
    @throws[NoSuchTargetException]
    @throws[UnknownProjectException]
    override def getTarget(identifier: TargetIdentifier): Target = {
        require(identifier != null && identifier.nonEmpty)

        identifier.project match {
            case None => throw new NoSuchTargetException(identifier)
            case Some(project) =>
                val child = getProjectContext(project)
                child.getTarget(identifier)
        }
    }

    /**
      * Returns a fully qualified connection from a project belonging to the namespace of this execution
      *
      * @param identifier
      * @return
      */
    @throws[InstantiateConnectionFailedException]
    @throws[NoSuchConnectionException]
    @throws[UnknownProjectException]
    override def getConnection(identifier:ConnectionIdentifier) : Connection = {
        require(identifier != null && identifier.nonEmpty)

        identifier.project match {
            case None =>
                connections.getOrElseUpdate(identifier.name,
                    extraConnections.get(identifier.name)
                        .orElse(
                            namespace
                                .flatMap(_.connections.get(identifier.name))
                        )
                        .map { p =>
                            try {
                                p.instantiate(this)
                            }
                            catch {
                                case NonFatal(ex) => throw new InstantiateConnectionFailedException(identifier, ex)
                            }
                        }
                        .getOrElse(throw new NoSuchConnectionException(identifier))
                )
            case Some(project) =>
                val child = getProjectContext(project)
                child.getConnection(identifier)
        }
    }

    /**
      * Returns a fully qualified job from a project belonging to the namespace of this execution
      *
      * @param identifier
      * @return
      */
    @throws[InstantiateJobFailedException]
    @throws[NoSuchJobException]
    @throws[UnknownProjectException]
    override def getJob(identifier: JobIdentifier): Job = {
        require(identifier != null && identifier.nonEmpty)

        identifier.project match {
            case None => throw new NoSuchJobException (identifier)
            case Some(project) =>
                val child = getProjectContext (project)
                child.getJob (identifier)
        }
    }

    /**
     * Returns a fully qualified test from a project belonging to the namespace of this execution
     *
     * @param identifier
     * @return
     */
    @throws[InstantiateTestFailedException]
    @throws[NoSuchTestException]
    @throws[UnknownProjectException]
    override def getTest(identifier: TestIdentifier): Test = {
        require(identifier != null && identifier.nonEmpty)

        identifier.project match {
            case None => throw new NoSuchTestException(identifier)
            case Some(project) =>
                val child = getProjectContext(project)
                child.getTest(identifier)
        }
    }

    /**
     * Returns a fully qualified template from a project belonging to the namespace of this execution
     *
     * @param identifier
     * @return
     */
    @throws[InstantiateTemplateFailedException]
    @throws[NoSuchTemplateException]
    @throws[UnknownProjectException]
    override def getTemplate(identifier: TemplateIdentifier): Template[_] = {
        require(identifier != null && identifier.nonEmpty)

        identifier.project match {
            case None => throw new NoSuchTemplateException(identifier)
            case Some(project) =>
                val child = getProjectContext(project)
                child.getTemplate(identifier)
        }
    }

    /**
     * Returns the context for a specific project. This will either return an existing context or create a new
     * one if it does not exist yet.
     *
     * @param project
     * @return
     */
    def getProjectContext(project:Project) : Context = {
        require(project != null)
        _children.getOrElseUpdate(project.name, createProjectContext(project))
    }

    /**
      * Returns the context for a specific project
      *
      * @param projectName
      * @return
      */
    def getProjectContext(projectName:String) : Context = {
        require(projectName != null && projectName.nonEmpty)

        lazy val project = {
            projects.find(_.name == projectName).getOrElse(throw new UnknownProjectException(projectName))
        }
        _children.getOrElseUpdate(projectName, createProjectContext(project))
    }

    private def createProjectContext(project: Project) : Context = {
        //if (!_projects.exists(_ eq project))
        //    throw new IllegalArgumentException(s"Project ${project.name} is not registered in execution context")

        val builder = ProjectContext.builder(this, project)
        // Apply all selected profiles defined in the project
        profiles.foreach { prof =>
            project.profiles.get(prof).foreach { profile =>
                builder.withProfile(profile)
            }
        }

        // We need to instantiate the projects job within its context, so we create a very temporary context
        def getImportJob(name:String) : Job = {
            try {
                val projectContext = ProjectContext.builder(this, project)
                    .withEnvironment(project.environment, SettingLevel.PROJECT_SETTING)
                    .build()
                projectContext.getJob(JobIdentifier(name))
            } catch {
                case NonFatal(ex) =>
                    throw new IllegalArgumentException(s"Cannot instantiate job '$name' to apply import settings for project ${project.name}", ex)
            }
        }

        // Apply any import setting
        _imports.get(project.name).foreach { case(context,imprt) =>
            val job = context.evaluate(imprt.job) match {
                case Some(name) =>
                    Some(getImportJob(name))
                case None =>
                    if (project.jobs.contains("main"))
                        Some(getImportJob("main"))
                    else None
            }
            job.foreach { job =>
                val args = job.arguments(context.evaluate(imprt.arguments))
                builder.withEnvironment(args, SettingLevel.SCOPE_OVERRIDE)
                builder.withEnvironment(job.environment, SettingLevel.JOB_OVERRIDE)
            }
        }

        // Apply overrides
        builder.overrideMappings(overrideMappings.filter(_._1.project.contains(project.name)).map(kv => (kv._1.name, kv._2)))
        builder.overrideRelations(overrideRelations.filter(_._1.project.contains(project.name)).map(kv => (kv._1.name, kv._2)))

        val context = builder
            .withEnvironment(project.environment, SettingLevel.PROJECT_SETTING)
            .withConfig(project.config)
            .build()

        // Store imports, together with context
        project.imports.foreach { im =>
            _imports.update(im.project, (context, im))
        }

        context
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
