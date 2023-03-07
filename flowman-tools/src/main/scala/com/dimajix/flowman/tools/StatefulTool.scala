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

package com.dimajix.flowman.tools

import com.dimajix.flowman.Tool
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Test


class StatefulTool(
    config:Map[String,String],
    environment:Map[String,String],
    profiles:Seq[String],
    sparkMaster:String,
    sparkName:String
) extends Tool {
    private var _project: Project = Project("empty")
    private var _job: Option[Job] = None
    private var _test: Option[Test] = None
    private var _context: Context = _
    private var _session: Session = _

    // Create new session
    newSession()

    def session: Session = _session

    def project: Project = _project

    def context: Context = _context

    def job: Option[Job] = _job

    def test: Option[Test] = _test

    def newSession() : Session = {
        if (_session != null) {
            _session.shutdown()
            _session = null
        }

        // Create Flowman Session, which also includes a Spark Session
        _session = super.createSession(
            sparkMaster,
            sparkName,
            project = Option(_project),
            additionalConfigs = config,
            additionalEnvironment = environment,
            profiles = profiles
        )
        _context = _session.getContext(project)
        _job = None
        _test = None
        _session
    }

    def loadProject(projectPath: String): Project = {
        val file = Tool.resolvePath(projectPath)
        loadProject(file)
    }

    def loadProject(file:File): Project = {
        // First try to load new project
        _project = Project.read.file(file)

        // Then create new session. If project loading fails, the old session will remain
        newSession()

        _project
    }

    def enterJob(job: Job, args:Map[String,String]): Unit = {
        val jargs = job.arguments(args)
        _context = _session.runner.withJobContext(job, jargs, Some(_session.execution), isolated=true) { (context,args) => context }
        _session.execution.cleanup()
        _test = None
        _job = Some(job)
    }

    def leaveJob(): Unit = {
        if (_job.nonEmpty) {
            _context = _session.getContext(project)
            _session.execution.cleanup()
            _job = None
        }
    }

    def enterTest(test: Test): Unit = {
        _context = _session.runner.withTestContext(test) { context => context }
        _session.execution.cleanup()
        _job = None
        _test = Some(test)
    }

    def leaveTest(): Unit = {
        if (_test.nonEmpty) {
            _context = _session.getContext(project)
            _session.execution.cleanup()
            _test = None
        }
    }
}
