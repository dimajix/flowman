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

import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.spec.connection.Connection
import com.dimajix.flowman.spec.connection.ConnectionSpec
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.flow.MappingSpec
import com.dimajix.flowman.spec.job.Job
import com.dimajix.flowman.spec.job.JobSpec
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.model.RelationSpec
import com.dimajix.flowman.spec.target.Target
import com.dimajix.flowman.spec.target.TargetSpec


object ScopeContext {
    class Builder(parent:Context) extends AbstractContext.Builder[Builder,ScopeContext](parent, SettingLevel.SCOPE_OVERRIDE) {
        require(parent != null)

        private var mappings = Map[String, MappingSpec]()
        private var relations = Map[String, RelationSpec]()
        private var targets = Map[String, TargetSpec]()
        private var jobs = Map[String, JobSpec]()

        def withMappings(mappings:Map[String,MappingSpec]) : Builder = {
            require(mappings != null)
            this.mappings = this.mappings ++ mappings
            this
        }
        def withRelations(relations:Map[String,RelationSpec]) : Builder = {
            require(relations != null)
            this.relations = this.relations ++ relations
            this
        }
        def withTargets(targets:Map[String,TargetSpec]) : Builder = {
            require(targets != null)
            this.targets = this.targets ++ targets
            this
        }
        def withJobs(jobs:Map[String,JobSpec]) : Builder = {
            require(jobs != null)
            this.jobs = this.jobs ++ jobs
            this
        }

        override protected val logger = LoggerFactory.getLogger(classOf[ScopeContext])

        override protected def createContext(env:Map[String,(Any, Int)], config:Map[String,(String, Int)], connections:Map[String, ConnectionSpec]) : ScopeContext = {
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


class ScopeContext(
    parent:Context,
    fullEnv:Map[String,(Any, Int)],
    fullConfig:Map[String,(String, Int)],
    scopeMappings:Map[String,MappingSpec] = Map(),
    scopeRelations:Map[String,RelationSpec] = Map(),
    scopeTargets:Map[String,TargetSpec] = Map(),
    scopeConnections:Map[String,ConnectionSpec] = Map(),
    scopeJobs:Map[String,JobSpec] = Map()
) extends AbstractContext(parent, fullEnv, fullConfig) {
    private val mappings = mutable.Map[String,Mapping]()
    private val relations = mutable.Map[String,Relation]()
    private val targets = mutable.Map[String,Target]()
    private val connections = mutable.Map[String,Connection]()
    private val jobs = mutable.Map[String,Job]()

    override def namespace: Namespace = parent.namespace
    override def project: Project = parent.project
    override def root: RootContext = parent.root
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
    override def getMapping(identifier: MappingIdentifier): Mapping = {
        if (identifier.project.isEmpty) {
            mappings.get(identifier.name) match {
                case Some(result) => result
                case None => scopeMappings.get(identifier.name) match {
                    case Some(spec) =>
                        val result = spec.instantiate(this)
                        mappings.put(identifier.name, result)
                        result
                    case None => parent.getMapping(identifier)
                }
            }
        }
        else {
            parent.getMapping(identifier)
        }
    }
    override def getRelation(identifier: RelationIdentifier): Relation = {
        if (identifier.project.isEmpty) {
            relations.get(identifier.name) match {
                case Some(result) => result
                case None => scopeRelations.get(identifier.name) match {
                    case Some(spec) =>
                        val result = spec.instantiate(this)
                        relations.put(identifier.name, result)
                        result
                    case None => parent.getRelation(identifier)
                }
            }
        }
        else {
            parent.getRelation(identifier)
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
