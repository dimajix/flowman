/*
 * Copyright 2022-2023 Kaya Kupferschmidt
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

import org.slf4j.LoggerFactory

import com.dimajix.flowman.fs.File
import com.dimajix.flowman.storage.Workspace


class SimpleWorkspaceManager(workspace:Workspace) extends WorkspaceManager {
    private val logger = LoggerFactory.getLogger(classOf[SimpleWorkspaceManager])

    // logger.info(s"Initialized SimpleWorkspaceManager at '$root'")

    override def list(): Seq[Workspace] = {
        Seq(workspace)
    }

    override def createWorkspace(name: String): Workspace = {
        throw new UnsupportedOperationException("Creating new Workspaces is not supported for the SimpleWorkspaceManager")
    }

    override def deleteWorkspace(name: String): Unit = {
        throw new UnsupportedOperationException("Deleting an existing Workspaces is not supported for the SimpleWorkspaceManager")
    }


    override def getWorkspace(name: String): Workspace = {
        if (name != workspace.name)
            throw new IllegalArgumentException(s"Workspace '$name' does not exists")
        workspace
    }
}
