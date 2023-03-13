/*
 * Copyright (C) 2021 The Flowman Authors
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

import scala.collection.mutable

import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.storage.Store


class SessionManager(val rootSession:execution.Session) {
    private val logger = LoggerFactory.getLogger(classOf[SessionManager])
    private val sessions = mutable.ListBuffer[SessionService]()

    /**
     * Returns a list of all active [[SessionService]]s
     * @return
     */
    def list() : Seq[SessionService] = {
        val result = mutable.ListBuffer[SessionService]()
        sessions.synchronized {
            result.append(sessions:_*)
        }
        result
    }

    /**
     * Returns the [[SessionService]] for a specific id. If no such session is known, [[None]] will be returned
     * instead
     * @param id
     * @return
     */
    def getSession(id:String) : Option[SessionService] = {
        var result:Option[SessionService] = None
        sessions.synchronized {
            result = sessions.find(_.id == id)
        }
        result
    }

    /**
     * Creates a new [[SessionService]] by loading a new project. The project is specified via its name, as returned
     * by [[rootSession]]
     * @param projectPath
     * @return
     */
    def createSession(store: Store, projectName:String, clientId:UUID) : SessionService = {
        val project = store.loadProject(projectName)
        createSession(store, project, clientId)
    }
    def createSession(store: Store, project:Project, clientId:UUID) : SessionService = {
        val svc = new SessionService(this, store, project, clientId)

        sessions.synchronized {
            sessions.append(svc)
        }

        svc
    }
    def createSession(store: Store, projectLocation: File, clientId:UUID): SessionService = {
        if (!projectLocation.isAbsolute())
            throw new IllegalArgumentException(s"Project location is not absolute: $projectLocation")

        val project = Project.read.file(projectLocation)
        createSession(store, project, clientId)
    }
    def createSession(projectLocation:File, clientId:UUID) : SessionService = {
        createSession(rootSession.store, projectLocation, clientId)
    }

    def removeClientSessions(clientId:UUID) : Unit = {
        val toBeRemoved = sessions.synchronized {
            sessions.filter(_.clientId == clientId)
        }
        toBeRemoved.foreach(_.close())
    }


    private[service] def removeSession(svc:SessionService) : Unit = {
        val id = svc.id
        sessions.synchronized {
            val index = sessions.indexWhere(_.id == id)
            if (index >= 0) {
                sessions.remove(index)
            }
        }
    }
}

