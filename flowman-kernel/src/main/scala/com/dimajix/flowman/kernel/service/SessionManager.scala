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

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import com.dimajix.flowman.execution
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.model.Project


class SessionManager(rootSession:execution.Session) {
    private val sessions = mutable.ListBuffer[SessionService]()

    def list() : Seq[SessionService] = {
        val result = mutable.ListBuffer[SessionService]()
        sessions.synchronized {
            result.append(sessions:_*)
        }
        result
    }

    def getSession(id:String) : SessionService = {
        var result:Option[SessionService] = None
        sessions.synchronized {
            result = sessions.find(_.id == id)
        }
        result.get
    }

    def createSession(projectPath:Path) : SessionService = {
        val project = loadProject(projectPath)
        val session = rootSession.newSession(project)

        val svc = new SessionService(session)

        sessions.synchronized {
            sessions.append(svc)
        }

        svc
    }

    def closeSession(svc:SessionService) : Unit = {
        val id = svc.id
        sessions.synchronized {
            val index = sessions.indexWhere(_.id == id)
            if (index >= 0) {
                sessions.remove(index)
            }
        }
    }

    private def loadProject(projectPath:Path) : Project = {
        // Create Hadoop FileSystem instance
        val hadoopConfig = new Configuration()
        val fs = FileSystem(hadoopConfig)

        // Load Project. If no schema is specified, load from local file system
        val projectUri = projectPath.toUri
        if (projectUri.getAuthority == null && projectUri.getScheme == null)
            Project.read.file(fs.local(projectPath))
        else
            Project.read.file(fs.file(projectPath))
    }
}
