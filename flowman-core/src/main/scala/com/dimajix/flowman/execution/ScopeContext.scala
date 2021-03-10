/*
 * Copyright 2019 Kaya Kupferschmidt
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

import org.slf4j.LoggerFactory

import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.Template


object ScopeContext {
    class Builder(parent:Context) extends AbstractContext.Builder[Builder,ScopeContext](parent, SettingLevel.SCOPE_OVERRIDE) {
        require(parent != null)

        private var mappings = Map[String, Template[Mapping]]()
        private var relations = Map[String, Template[Relation]]()
        private var targets = Map[String, Template[Target]]()
        private var jobs = Map[String, Template[Job]]()

        def withMappings(mappings:Map[String,Template[Mapping]]) : Builder = {
            require(mappings != null)
            this.mappings = this.mappings ++ mappings
            this
        }
        def withRelations(relations:Map[String,Template[Relation]]) : Builder = {
            require(relations != null)
            this.relations = this.relations ++ relations
            this
        }
        def withTargets(targets:Map[String,Template[Target]]) : Builder = {
            require(targets != null)
            this.targets = this.targets ++ targets
            this
        }
        def withJobs(jobs:Map[String,Template[Job]]) : Builder = {
            require(jobs != null)
            this.jobs = this.jobs ++ jobs
            this
        }

        override protected val logger = LoggerFactory.getLogger(classOf[ScopeContext])

        override protected def createContext(env:Map[String,(Any, Int)], config:Map[String,(String, Int)], connections:Map[String, Template[Connection]]) : ScopeContext = {
            new ScopeContext(
                parent,
                env,
                config,
                mappings,
                relations,
                targets,
                connections,
                jobs
            )
        }
    }

    def builder(parent:Context) : Builder = new Builder(parent)
}


final class ScopeContext(
    parent:Context,
    fullEnv:Map[String,(Any, Int)],
    fullConfig:Map[String,(String, Int)],
    scopeMappings:Map[String,Template[Mapping]] = Map(),
    scopeRelations:Map[String,Template[Relation]] = Map(),
    scopeTargets:Map[String,Template[Target]] = Map(),
    scopeConnections:Map[String,Template[Connection]] = Map(),
    scopeJobs:Map[String,Template[Job]] = Map()
) extends AbstractContext(fullEnv, fullConfig) {
    private val mappings = mutable.Map[String,Mapping]()
    private val relations = mutable.Map[String,Relation]()
    private val targets = mutable.Map[String,Target]()
    private val connections = mutable.Map[String,Connection]()
    private val jobs = mutable.Map[String,Job]()

    /**
     * Returns the namespace associated with this context. Can be null
     * @return
     */
    override def namespace: Option[Namespace] = parent.namespace

    /**
     * Returns the project associated with this context. Can be [[None]]
     * @return
     */
    override def project: Option[Project] = parent.project

    /**
     * Returns the root context in a hierarchy of connected contexts
     * @return
     */
    override def root: RootContext = parent.root

    /**
     * Returns the list of active profile names
     *
     * @return
     */
    override def profiles: Set[String] = parent.profiles

    override def getConnection(identifier: ConnectionIdentifier): Connection = {
        if (identifier.project.isEmpty) {
            connections.get(identifier.name) match {
                case Some(result) => result
                case None => scopeConnections.get(identifier.name) match {
                    case Some(spec) =>
                        val result = spec.instantiate(this)
                        connections.put(identifier.name, result)
                        result
                    case None => parent.getConnection(identifier)
                }
            }
        }
        else {
            parent.getConnection(identifier)
        }
    }
    override def getMapping(identifier: MappingIdentifier, allowOverrides:Boolean=true): Mapping = {
        if (identifier.project.isEmpty) {
            mappings.get(identifier.name) match {
                case Some(result) => result
                case None => scopeMappings.get(identifier.name) match {
                    case Some(spec) =>
                        val result = spec.instantiate(this)
                        mappings.put(identifier.name, result)
                        result
                    case None => parent.getMapping(identifier, allowOverrides)
                }
            }
        }
        else {
            parent.getMapping(identifier, allowOverrides)
        }
    }
    override def getRelation(identifier: RelationIdentifier, allowOverrides:Boolean=true): Relation = {
        if (identifier.project.isEmpty) {
            relations.get(identifier.name) match {
                case Some(result) => result
                case None => scopeRelations.get(identifier.name) match {
                    case Some(spec) =>
                        val result = spec.instantiate(this)
                        relations.put(identifier.name, result)
                        result
                    case None => parent.getRelation(identifier, allowOverrides)
                }
            }
        }
        else {
            parent.getRelation(identifier, allowOverrides)
        }
    }
    override def getTarget(identifier: TargetIdentifier): Target = {
        if (identifier.project.isEmpty) {
            targets.get(identifier.name) match {
                case Some(result) => result
                case None => scopeTargets.get(identifier.name) match {
                    case Some(spec) =>
                        val result = spec.instantiate(this)
                        targets.put(identifier.name, result)
                        result
                    case None => parent.getTarget(identifier)
                }
            }
        }
        else {
            parent.getTarget(identifier)
        }
    }
    override def getJob(identifier: JobIdentifier): Job = {
        if (identifier.project.isEmpty) {
            jobs.get(identifier.name) match {
                case Some(result) => result
                case None => scopeJobs.get(identifier.name) match {
                    case Some(spec) =>
                        val result = spec.instantiate(this)
                        jobs.put(identifier.name, result)
                        result
                    case None => parent.getJob(identifier)
                }
            }
        }
        else {
            parent.getJob(identifier)
        }
    }
}
