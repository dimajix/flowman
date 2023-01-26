/*
 * Copyright 2019-2023 Kaya Kupferschmidt
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
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestIdentifier


object ScopeContext {
    class Builder(parent:Context) extends AbstractContext.Builder[Builder,ScopeContext](Some(parent), SettingLevel.SCOPE_OVERRIDE) {
        require(parent != null)

        private var mappings = Map[String, Prototype[Mapping]]()
        private var relations = Map[String, Prototype[Relation]]()
        private var targets = Map[String, Prototype[Target]]()
        private var jobs = Map[String, Prototype[Job]]()
        private var tests = Map[String, Prototype[Test]]()

        def withMappings(mappings:Map[String,Prototype[Mapping]]) : Builder = {
            require(mappings != null)
            this.mappings = this.mappings ++ mappings
            this
        }
        def withRelations(relations:Map[String,Prototype[Relation]]) : Builder = {
            require(relations != null)
            this.relations = this.relations ++ relations
            this
        }
        def withTargets(targets:Map[String,Prototype[Target]]) : Builder = {
            require(targets != null)
            this.targets = this.targets ++ targets
            this
        }
        def withJobs(jobs:Map[String,Prototype[Job]]) : Builder = {
            require(jobs != null)
            this.jobs = this.jobs ++ jobs
            this
        }
        def withTests(tests:Map[String,Prototype[Test]]) : Builder = {
            require(tests != null)
            this.tests = this.tests ++ tests
            this
        }

        override protected val logger = LoggerFactory.getLogger(classOf[ScopeContext])

        override protected def createContext(env:Map[String,(Any, Int)], config:Map[String,(String, Int)], connections:Map[String, Prototype[Connection]]) : ScopeContext = {
            new ScopeContext(
                parent,
                env,
                config,
                mappings,
                relations,
                targets,
                connections,
                jobs,
                tests
            )
        }
    }

    def builder(parent:Context) : Builder = new Builder(parent)
}


final class ScopeContext(
    parent:Context,
    fullEnv:Map[String,(Any, Int)],
    fullConfig:Map[String,(String, Int)],
    scopeMappings:Map[String,Prototype[Mapping]] = Map(),
    scopeRelations:Map[String,Prototype[Relation]] = Map(),
    scopeTargets:Map[String,Prototype[Target]] = Map(),
    scopeConnections:Map[String,Prototype[Connection]] = Map(),
    scopeJobs:Map[String,Prototype[Job]] = Map(),
    scopeTests:Map[String,Prototype[Test]] = Map()
) extends AbstractContext(fullEnv, fullConfig) {
    private val mappings = TrieMap[String,Mapping]()
    private val relations = TrieMap[String,Relation]()
    private val targets = TrieMap[String,Target]()
    private val connections = TrieMap[String,Connection]()
    private val jobs = TrieMap[String,Job]()
    private val tests = TrieMap[String,Test]()

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

    @throws[InstantiateConnectionFailedException]
    @throws[NoSuchConnectionException]
    @throws[UnknownProjectException]
    override def getConnection(identifier: ConnectionIdentifier): Connection = {
        if (identifier.project.isEmpty) {
            connections.get(identifier.name) match {
                case Some(result) => result
                case None => scopeConnections.get(identifier.name) match {
                    case Some(spec) =>
                        connections.getOrElseUpdate(identifier.name,
                            try {
                                spec.instantiate(this)
                            }
                            catch {
                                case NonFatal(ex) => throw new InstantiateConnectionFailedException(identifier, ex)
                            }
                        )
                    case None => parent.getConnection(identifier)
                }
            }
        }
        else {
            parent.getConnection(identifier)
        }
    }

    @throws[InstantiateMappingFailedException]
    @throws[NoSuchMappingException]
    @throws[UnknownProjectException]
    override def getMapping(identifier: MappingIdentifier, allowOverrides:Boolean=true): Mapping = {
        if (identifier.project.isEmpty) {
            mappings.get(identifier.name) match {
                case Some(result) => result
                case None => scopeMappings.get(identifier.name) match {
                    case Some(spec) =>
                        mappings.getOrElseUpdate(identifier.name,
                            try {
                                spec.instantiate(this)
                            }
                            catch {
                                case NonFatal(ex) => throw new InstantiateMappingFailedException(identifier, ex)
                            }
                        )
                    case None => parent.getMapping(identifier, allowOverrides)
                }
            }
        }
        else {
            parent.getMapping(identifier, allowOverrides)
        }
    }

    @throws[InstantiateRelationFailedException]
    @throws[NoSuchRelationException]
    @throws[UnknownProjectException]
    override def getRelation(identifier: RelationIdentifier, allowOverrides:Boolean=true): Relation = {
        if (identifier.project.isEmpty) {
            relations.get(identifier.name) match {
                case Some(result) => result
                case None => scopeRelations.get(identifier.name) match {
                    case Some(spec) =>
                        relations.getOrElseUpdate(identifier.name,
                            try {
                                spec.instantiate(this)
                            }
                            catch {
                                case NonFatal(ex) => throw new InstantiateRelationFailedException(identifier, ex)
                            })
                    case None => parent.getRelation(identifier, allowOverrides)
                }
            }
        }
        else {
            parent.getRelation(identifier, allowOverrides)
        }
    }

    @throws[InstantiateTargetFailedException]
    @throws[NoSuchTargetException]
    @throws[UnknownProjectException]
    override def getTarget(identifier: TargetIdentifier): Target = {
        if (identifier.project.isEmpty) {
            targets.get(identifier.name) match {
                case Some(result) => result
                case None => scopeTargets.get(identifier.name) match {
                    case Some(spec) =>
                        targets.getOrElseUpdate(identifier.name,
                            try {
                                spec.instantiate(this)
                            }
                            catch {
                                case NonFatal(ex) => throw new InstantiateTargetFailedException(identifier, ex)
                            })
                    case None => parent.getTarget(identifier)
                }
            }
        }
        else {
            parent.getTarget(identifier)
        }
    }

    @throws[InstantiateJobFailedException]
    @throws[NoSuchJobException]
    @throws[UnknownProjectException]
    override def getJob(identifier: JobIdentifier): Job = {
        if (identifier.project.isEmpty) {
            jobs.get(identifier.name) match {
                case Some(result) => result
                case None => scopeJobs.get(identifier.name) match {
                    case Some(spec) =>
                        jobs.getOrElseUpdate(identifier.name,
                            try {
                                spec.instantiate(this)
                            }
                            catch {
                                case NonFatal(ex) => throw new InstantiateJobFailedException(identifier, ex)
                            })
                    case None => parent.getJob(identifier)
                }
            }
        }
        else {
            parent.getJob(identifier)
        }
    }

    @throws[InstantiateTestFailedException]
    @throws[NoSuchTestException]
    @throws[UnknownProjectException]
    override def getTest(identifier: TestIdentifier): Test = {
        if (identifier.project.isEmpty) {
            tests.get(identifier.name) match {
                case Some(result) => result
                case None => scopeTests.get(identifier.name) match {
                    case Some(spec) =>
                        tests.getOrElseUpdate(identifier.name,
                            try {
                                spec.instantiate(this)
                            }
                            catch {
                                case NonFatal(ex) => throw new InstantiateTestFailedException(identifier, ex)
                            })
                    case None => parent.getTest(identifier)
                }
            }
        }
        else {
            parent.getTest(identifier)
        }
    }

    /**
     * Returns a specific named [[Template]]. The Test can either be inside this Contexts project or in a different
     * project within the same namespace
     *
     * @param identifier
     * @return
     */
    @throws[InstantiateTemplateFailedException]
    @throws[NoSuchTemplateException]
    @throws[UnknownProjectException]
    override def getTemplate(identifier: TemplateIdentifier): Template[_] = parent.getTemplate(identifier)
}
