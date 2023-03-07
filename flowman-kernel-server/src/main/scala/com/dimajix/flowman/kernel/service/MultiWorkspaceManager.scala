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

import scala.collection.mutable

import org.slf4j.LoggerFactory

import com.dimajix.flowman.fs.File
import com.dimajix.flowman.spec.storage.SimpleWorkspace
import com.dimajix.flowman.storage.Workspace


class MultiWorkspaceManager(root:File) extends WorkspaceManager {
    private val logger = LoggerFactory.getLogger(classOf[MultiWorkspaceManager])
    private val workspaces: mutable.Map[String, SimpleWorkspace] = mutable.Map()

    logger.info(s"Initialized MultiWorkspaceManager at '$root'")

    // Create workspace are + default workspace
    root.mkdirs()
    workspaces.put("default", SimpleWorkspace.create(root / "default"))


    override def list(): Seq[Workspace] = {
        val ws = SimpleWorkspace.list(root)

        workspaces.synchronized {
            workspaces.clear()
            ws.foreach(ws => workspaces.put(ws.name, ws))
        }

        ws
    }

    override def createWorkspace(name: String): Workspace = {
        val path = root / name
        if (SimpleWorkspace.exists(path))
            throw new IllegalArgumentException(s"Workspace '$name' already exists")
        val ws = SimpleWorkspace.create(path)

        workspaces.synchronized {
            workspaces.put(name, ws)
        }

        ws
    }

    override def deleteWorkspace(name: String): Unit = {
        val path = root / name
        if (!SimpleWorkspace.exists(path))
            throw new IllegalArgumentException(s"Workspace '$name' does not exists")

        logger.info(s"Deleting workspace '$name' at '$path'")
        workspaces.synchronized {
            workspaces.remove(name)
        }

        path.delete(true)
    }

    override def getWorkspace(name: String): Workspace = {
        workspaces.synchronized {
            workspaces.getOrElseUpdate(name, {
                val path = root / name
                if (!SimpleWorkspace.exists(path))
                    throw new IllegalArgumentException(s"Workspace '$name' does not exists")
                val ws = SimpleWorkspace.load(path)
                ws
            })
        }
    }
}
