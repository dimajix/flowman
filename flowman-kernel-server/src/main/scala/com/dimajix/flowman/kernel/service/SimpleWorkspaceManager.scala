/*
 * Copyright (C) 2022 The Flowman Authors
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

import org.slf4j.LoggerFactory

import com.dimajix.flowman.storage.Workspace


class SimpleWorkspaceManager(workspace:Workspace) extends WorkspaceManager {
    private val logger = LoggerFactory.getLogger(classOf[SimpleWorkspaceManager])

    // logger.info(s"Initialized SimpleWorkspaceManager at '$root'")

    override def list(): Seq[(String,Workspace)] = {
        Seq(workspace.name -> workspace)
    }

    override def createWorkspace(id: String, name:String, clientId:Option[UUID]): (String,Workspace) = {
        throw new UnsupportedOperationException("Creating new Workspaces is not supported for the SimpleWorkspaceManager")
    }

    override def deleteWorkspace(id: String): Unit = {
        throw new UnsupportedOperationException("Deleting an existing Workspaces is not supported for the SimpleWorkspaceManager")
    }


    override def getWorkspace(id: String): (String,Workspace) = {
        if (id != workspace.name)
            throw new IllegalArgumentException(s"Workspace '$id' does not exists")
        id -> workspace
    }

    override def removeClientWorkspaces(clientId: UUID): Unit = {
    }
}
