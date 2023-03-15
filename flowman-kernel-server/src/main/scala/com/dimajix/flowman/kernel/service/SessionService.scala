/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.flowman.kernel.service

import java.io.Closeable
import java.time.Clock
import java.util.UUID

import scala.concurrent.ExecutionContext

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.JobCoordinator
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Runner
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.execution.TestCoordinator
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestIdentifier
import com.dimajix.flowman.model.TestIdentifier
import com.dimajix.flowman.spec.mapping.SqlMapping
import com.dimajix.flowman.spec.target.RelationTarget
import com.dimajix.flowman.storage.Store
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StructType


class SessionService(sessionManager:SessionManager, val store:Store, val project:Project, val clientId:UUID) extends Closeable {
    private val logger = LoggerFactory.getLogger(classOf[SessionService])
    val loggerFactory : ForwardingLoggerFactory = new ForwardingLoggerFactory()
    val session : Session = Session.builder(sessionManager.rootSession)
        .withLoggerFactory(loggerFactory)
        .withProject(project)
        .withStore(store)
        .build()

    private var _job: Option[Job] = None
    private var _jobArgs: Map[String,String] = Map.empty
    private var _test: Option[Test] = None
    private var _context : Context = session.getContext(session.project.get)

    val id : String = UUID.randomUUID().toString
    val name : String = project.name
    val namespace : Namespace = session.namespace.get

    logger.info(s"Create new SessionService for client $clientId with session id '$id'")

    def context : Context = _context
    def execution : Execution = session.execution
    def runner: Runner = session.runner

    override def close(): Unit = {
        session.execution.cleanup()
        session.shutdown()
        sessionManager.removeSession(this)
        logger.info(s"Shutdown SessionService for client $clientId with session id '$id'")
    }

    def job: Option[Job] = _job
    def jobArgs: Map[String,String] = _jobArgs
    def test: Option[Test] = _test

    def reset() : Unit = {
        _context = session.getContext(project)
        session.execution.cleanup()
        _job = None
        _jobArgs = Map.empty
        _test = None
    }

    def listJobs() : Seq[JobIdentifier] = {
        project.jobs.keys.map(JobIdentifier(_)).toSeq
    }
    def getJob(name:JobIdentifier) : Job = {
        context.getJob(name)
    }
    def enterJob(job: Job, args:Map[String,String]): Unit = {
        val jargs = job.arguments(args)
        _context = runner.withJobContext(job, jargs, Some(session.execution), isolated=true) { (context,args) => context }
        session.execution.cleanup()
        _test = None
        _job = Some(job)
        _jobArgs = args
    }
    def leaveJob(): Unit = {
        _context = session.getContext(project)
        session.execution.cleanup()
        _job = None
        _jobArgs = Map.empty
        _test = None
    }
    def executeJob(job:Job, lifecycle:Seq[Phase], args:Map[String,String],targets:Seq[String],dirtyTargets:Seq[String],force:Boolean, keepGoing:Boolean, dryRun:Boolean, parallelism:Int) : Status = {
        val coordinator = new JobCoordinator(session, force, keepGoing, dryRun, parallelism)
        coordinator.execute(job, lifecycle, job.parseArguments(args), targets.map(_.r), dirtyTargets = dirtyTargets.map(_.r))
    }

    def listTargets() : Seq[TargetIdentifier] = {
        project.targets.keys.map(TargetIdentifier(_)).toSeq
    }
    def getTarget(name:TargetIdentifier) : Target = {
        _context.getTarget(name)
    }
    def executeTargets(targets: Seq[Target], lifecycle: Seq[Phase], force: Boolean, keepGoing: Boolean, dryRun: Boolean): Status = {
        val runner = session.runner
        runner.executeTargets(targets, lifecycle, jobName = "cli-tools", force = force, keepGoing = keepGoing, dryRun = dryRun, isolated = false)
    }

    def listTests() : Seq[TestIdentifier] = {
        project.tests.keys.map(TestIdentifier(_)).toSeq
    }
    def getTest(name:TestIdentifier) : Test = {
        _context.getTest(name)
    }
    def executeTests(tests: Seq[TestIdentifier], keepGoing: Boolean, dryRun: Boolean, parallelism:Int) : Status = {
        val coordinator = new TestCoordinator(session, keepGoing, dryRun, parallelism)
        coordinator.execute(project, tests)
    }
    def enterTest(test: Test): Unit = {
        _context = session.runner.withTestContext(test) { context => context }
        session.execution.cleanup()
        _job = None
        _jobArgs = Map.empty
        _test = Some(test)
    }
    def leaveTest(): Unit = {
        _context = session.getContext(project)
        session.execution.cleanup()
        _job = None
        _jobArgs = Map.empty
        _test = None
    }

    def listMappings() : Seq[MappingIdentifier] = {
        project.mappings.keys.map(MappingIdentifier(_)).toSeq
    }
    def getMapping(name:MappingIdentifier) : Mapping = {
        _context.getMapping(name)
    }
    def describeMapping(mapping:Mapping, output:String, useSpark:Boolean) : StructType = {
        val execution = session.execution

        if (useSpark) {
            val df = execution.instantiate(mapping, output)
            StructType.of(df.schema)
        }
        else {
            execution.describe(mapping, output)
        }
    }

    def listRelations() : Seq[RelationIdentifier] = {
        project.relations.keys.map(RelationIdentifier(_)).toSeq
    }
    def getRelation(name:RelationIdentifier) : Relation = {
        _context.getRelation(name)
    }
    def executeRelations(relations: Seq[Relation], phase:Phase, partition:Map[String,String], force: Boolean, keepGoing: Boolean, dryRun: Boolean) : Status = {
        // Create a new project with appropriate build targets
        val targets = relations.map { rel =>
            rel.name -> Prototype.of { context =>
                val props = Target.Properties(context, rel.name, "relation")
                RelationTarget(props, rel, MappingOutputIdentifier.empty, partition).asInstanceOf[Target]
            }
        }
        val prj = project.copy(targets = project.targets ++ targets.toMap)
        val ctx = context.root.getProjectContext(prj)
        val targets2 = relations.map { rel => ctx.getTarget(TargetIdentifier(rel.name)) }

        val runner = session.runner
        runner.executeTargets(targets2, Seq(phase), jobName = "cli-tools", force = force, keepGoing = keepGoing, dryRun = dryRun, isolated = false)
    }

    def describeRelation(relation:Relation, partition:Map[String,String], useSpark:Boolean) : StructType = {
        val partition0 = partition.map { case (k, v) => k -> SingleValue(v) }
        val execution = session.execution

        if (useSpark) {
            val df = relation.read(session.execution, partition0)
            StructType.of(df.schema)
        }
        else {
            relation.describe(execution, partition0)
        }
    }

    def executeSql(statement:String) : DataFrame = {
        val mapping = SqlMapping(
            Mapping.Properties(context, "sql-" + Clock.systemUTC().millis()),
            sql = Some(statement)
        )
        val executor = session.execution
        executor.instantiate(mapping, "main")
    }
}
