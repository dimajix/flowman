/*
 * Copyright 2021 Kaya Kupferschmidt
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

import java.util.UUID

import scala.concurrent.ExecutionContext

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Runner
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestIdentifier


class SessionService(_session:Session) {
    private var _job: Option[Job] = None
    private var _test: Option[Test] = None
    private var _context : Context = _session.getContext(_session.project.get)

    val tasks = new TaskService(this)

    def executionContext:ExecutionContext = ???

    val id : String = UUID.randomUUID().toString
    val namespace : Namespace = _session.namespace.get
    val project : Project = _session.project.get

    def session : Session = _session
    def context : Context = _context
    def execution : Execution = _session.execution
    def runner: Runner = _session.runner

    def job: Option[Job] = _job
    def test: Option[Test] = _test

    def reset() : Unit = {
        _context = _session.getContext(project)
        _session.execution.cleanup()
        _job = None
        _test = None
    }

    def listJobs() : Seq[String] = project.jobs.keys.toSeq
    def getJob(name:String) : Job = {
        context.getJob(JobIdentifier(name))
    }
    def enterJob(job: Job, args:Map[String,String]): Unit = {
        val jargs = job.arguments(args)
        _context = runner.withJobContext(job,jargs) { (context,args) => context }
        _session.execution.cleanup()
        _test = None
        _job = Some(job)
    }
    def leaveJob(): Unit = {
        _context = _session.getContext(project)
        _session.execution.cleanup()
        _job = None
        _test = None
    }

    def listTargets() : Seq[String] = project.targets.keys.toSeq
    def getTarget(name:String) : Target = {
        _context.getTarget(TargetIdentifier(name))
    }

    def listTests() : Seq[String] = project.tests.keys.toSeq
    def getTest(name:String) : Test = {
        _context.getTest(TestIdentifier(name))
    }
    def enterTest(test: Test): Unit = {
        _context = _session.runner.withTestContext(test) { context => context }
        _session.execution.cleanup()
        _job = None
        _test = Some(test)
    }
    def leaveTest(): Unit = {
        _context = _session.getContext(project)
        _session.execution.cleanup()
        _job = None
        _test = None
    }

    def getMapping(name:String) : Mapping = {
        _context.getMapping(MappingIdentifier(name))
    }
    def collectMapping(mapping:Mapping, output:String) = ???
    def describeMapping(mapping:Mapping, output:String) = ???

    def getRelation(name:String) : Relation = ???
    def collectRelation(relation:Relation) = ???
}
