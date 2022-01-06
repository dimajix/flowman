/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

import org.slf4j.LoggerFactory

import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.model.Identifier
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Profile
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.ProjectWrapper
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestIdentifier


object ProjectContext {
    class Builder private[ProjectContext](parent:Context, project:Project) extends AbstractContext.Builder[Builder,ProjectContext](parent, SettingLevel.PROJECT_SETTING) {
        require(parent != null)
        require(project != null)

        override protected val logger = LoggerFactory.getLogger(classOf[ProjectContext])
        private var overrideMappings:Map[String, Prototype[Mapping]] = Map()
        private var overrideRelations:Map[String, Prototype[Relation]] = Map()

        override def withProfile(profile:Profile) : Builder = {
            withProfile(profile, SettingLevel.PROJECT_PROFILE)
            this
        }

        /**
         * Add extra mappings, which potentially override existing project mappings
         * @param mappings
         * @return
         */
        def overrideMappings(mappings:Map[String,Prototype[Mapping]]) : Builder = {
            overrideMappings = overrideMappings ++ mappings
            this
        }

        /**
         * Adds extra relations, which potentially override existing project relations
         * @param relations
         * @return
         */
        def overrideRelations(relations:Map[String,Prototype[Relation]]) : Builder = {
            overrideRelations = overrideRelations ++ relations
            this
        }

        override protected def createContext(env:Map[String,(Any, Int)], config:Map[String,(String, Int)], connections:Map[String, Prototype[Connection]]) : ProjectContext = {
            new ProjectContext(parent, project, env, config, connections, overrideMappings, overrideRelations)
        }
    }

    def builder(parent:Context, project:Project) = new Builder(parent, project)
}


/**
  * Execution context for a specific Flowman project. This will interpolate all resources within the project
  * or (if the resource is fully qualified) walks up into the parent context.
  * @param parent
  * @param _project
  */
final class ProjectContext private[execution](
    parent:Context,
    _project:Project,
    _env:Map[String,(Any, Int)],
    _config:Map[String,(String, Int)],
    extraConnections:Map[String, Prototype[Connection]],
    overrideMappingTemplates:Map[String, Prototype[Mapping]],
    overrideRelationTemplates:Map[String, Prototype[Relation]]
) extends AbstractContext(
    _env + ("project" -> ((ProjectWrapper(_project), SettingLevel.SCOPE_OVERRIDE.level))),
    _config)
{
    private val mappings = TrieMap[String,Mapping]()
    private val overrideMappings = TrieMap[String,Mapping]()
    private val relations = TrieMap[String,Relation]()
    private val overrideRelations = TrieMap[String,Relation]()
    private val targets = TrieMap[String,Target]()
    private val connections = TrieMap[String,Connection]()
    private val jobs = TrieMap[String,Job]()
    private val tests = TrieMap[String,Test]()
    private val templates = TrieMap[String,Template[_]]()

    /**
      * Returns the namespace associated with this context. Can be null
      * @return
      */
    override def namespace : Option[Namespace] = parent.namespace

    /**
     * Returns the project associated with this context. Can be [[None]]
     * @return
     */
    override def project : Option[Project] = Some(_project)

    /**
      * Returns the root context in a hierarchy of connected contexts
      * @return
      */
    override def root : RootContext = parent.root

    /**
     * Returns the list of active profile names
     *
     * @return
     */
    override def profiles: Set[String] = parent.profiles

    /**
      * Returns a specific named Transform. The Transform can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    override def getMapping(identifier: MappingIdentifier, allowOverrides:Boolean=true): Mapping = {
        require(identifier != null && identifier.nonEmpty)

        def findOverride() = {
            if (allowOverrides) {
                findOrInstantiate(identifier, overrideMappingTemplates, overrideMappings)
            }
            else {
                None
            }
        }
        def find() = {
            findOrInstantiate(identifier, _project.mappings, mappings)
        }

        if (identifier.project.forall(_ == _project.name)) {
            findOverride().orElse(find()).getOrElse(throw new NoSuchMappingException(identifier))
        }
        else {
            parent.getMapping(identifier, allowOverrides)
        }
    }

    /**
      * Returns a specific named RelationType. The RelationType can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    override def getRelation(identifier: RelationIdentifier, allowOverrides:Boolean=true): Relation = {
        require(identifier != null && identifier.nonEmpty)

        def findOverride() = {
            if (allowOverrides) {
                findOrInstantiate(identifier, overrideRelationTemplates, overrideRelations)
            }
            else {
                None
            }
        }
        def find() = {
            findOrInstantiate(identifier, _project.relations, relations)
        }

        if (identifier.project.forall(_ == _project.name)) {
            findOverride().orElse(find()).getOrElse(throw new NoSuchRelationException(identifier))
        }
        else {
            parent.getRelation(identifier, allowOverrides)
        }
    }

    /**
      * Returns a specific named TargetType. The TargetType can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    override def getTarget(identifier: TargetIdentifier): Target = {
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.forall(_ == _project.name)) {
            targets.getOrElseUpdate(identifier.name,
                _project.targets
                    .getOrElse(identifier.name,
                        throw new NoSuchTargetException(identifier)
                    )
                    .instantiate(this)
            )
        }
        else {
            parent.getTarget(identifier)
        }
    }

    /**
      * Try to retrieve the specified database. Performs lookups in parent context if required
      *
      * @param identifier
      * @return
      */
    override def getConnection(identifier:ConnectionIdentifier) : Connection = {
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.contains(_project.name)) {
            // Case 1: Project identifier explicitly set. Only look inside project
            connections.getOrElseUpdate(identifier.name,
                extraConnections.getOrElse(identifier.name,
                    _project.connections.getOrElse(identifier.name,
                        throw new NoSuchConnectionException(identifier)
                    )
                )
                .instantiate(this)
            )
        }
        else if (identifier.project.isEmpty) {
            // Case 2: Project identifier not set. Look in project and in parent.
            connections.getOrElse(identifier.name,
                extraConnections.get(identifier.name)
                    .orElse(_project.connections.get(identifier.name))
                    .map(t => connections.getOrElseUpdate(identifier.name, t.instantiate(this)))
                    .getOrElse(parent.getConnection(identifier))
            )
        }
        else {
            // Case 3: Project identifier set to different project
            parent.getConnection(identifier)
        }
    }

    /**
      * Returns a specific named Job. The job can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    override def getJob(identifier: JobIdentifier): Job = {
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.forall(_ == _project.name)) {
            jobs.getOrElseUpdate(identifier.name,
                _project.jobs
                    .getOrElse(identifier.name,
                        throw new NoSuchJobException(identifier)
                    )
                    .instantiate(this)
            )
        }
        else {
            parent.getJob(identifier)
        }
    }

    /**
     * Returns a specific named Test. The test can either be inside this Contexts project or in a different
     * project within the same namespace
     *
     * @param identifier
     * @return
     */
    override def getTest(identifier: TestIdentifier): Test = {
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.forall(_ == _project.name)) {
            tests.getOrElseUpdate(identifier.name,
                _project.tests
                    .getOrElse(identifier.name,
                        throw new NoSuchTestException(identifier)
                    )
                    .instantiate(this)
            )
        }
        else {
            parent.getTest(identifier)
        }
    }

    /**
     * Returns a specific named Template. The template can either be inside this Contexts project or in a different
     * project within the same namespace
     *
     * @param identifier
     * @return
     */
    override def getTemplate(identifier: TemplateIdentifier): Template[_] = {
        require(identifier != null && identifier.nonEmpty)

        if (identifier.project.forall(_ == _project.name)) {
            templates.getOrElseUpdate(identifier.name,
                _project.templates
                    .getOrElse(identifier.name,
                        throw new NoSuchTemplateException(identifier)
                    )
                    .instantiate(this)
            )
        }
        else {
            parent.getTemplate(identifier)
        }
    }

    private def findOrInstantiate[T](identifier:Identifier[T], prototypes:Map[String,Prototype[T]], cache:TrieMap[String,T]) = {
        val name = identifier.name
        cache.get(name)
            .orElse {
                prototypes
                    .get(name)
                    .map(m => cache.getOrElseUpdate(name, m.instantiate(this)))
            }
    }

}
