/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.studio.service

import scala.collection.mutable

import org.slf4j.LoggerFactory

import com.dimajix.flowman.fs.File
import com.dimajix.flowman.spec.storage.LocalWorkspace
import com.dimajix.flowman.storage.Workspace


class WorkspaceManager(root:File) {
    private val logger = LoggerFactory.getLogger(classOf[WorkspaceManager])
    private val workspaces : mutable.Map[String,Workspace] = mutable.Map()

    logger.info(s"Initialized WorkspaceManager at '$root'")

    // Create workspace are + default workspace
    root.mkdirs()
    workspaces.put("default", new LocalWorkspace(root / "default"))


    def list() : Seq[Workspace] = {
        val ws = LocalWorkspace.list(root)

        workspaces.synchronized {
            workspaces.clear()
            ws.foreach(ws => workspaces.put(ws.name, ws))
        }

        ws
    }

    def createWorkspace(name:String) : Workspace = {
        val path = root / name
        if (LocalWorkspace.exists(path))
            throw new IllegalArgumentException(s"Workspace '$name' already exists")
        val ws = new LocalWorkspace(path)

        workspaces.synchronized {
            workspaces.put(name, ws)
        }

        ws
    }

    def deleteWorkspace(name:String) : Unit = {
        val path = root / name
        if (!LocalWorkspace.exists(path))
            throw new IllegalArgumentException(s"Workspace '$name' does not exists")

        workspaces.synchronized {
            workspaces.remove(name)
        }

        path.delete(true)
    }

    def getWorkspace(name:String) : Workspace = {
        workspaces.synchronized {
            workspaces.getOrElseUpdate(name, {
                val path = root / name
                if (!LocalWorkspace.exists(path))
                    throw new IllegalArgumentException(s"Workspace '$name' does not exists")
                val ws = LocalWorkspace.load(path)
                ws
            })
        }
    }
}
