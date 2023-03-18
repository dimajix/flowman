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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.spec.storage.SimpleWorkspace
import com.dimajix.flowman.storage.Workspace


class MultiWorkspaceManager(root:File) extends WorkspaceManager {
    private val logger = LoggerFactory.getLogger(classOf[MultiWorkspaceManager])
    private val workspaces: mutable.Map[String, (Option[UUID],SimpleWorkspace)] = mutable.Map()

    logger.info(s"Initialized MultiWorkspaceManager at '$root'")

    // Create workspace are + default workspace
    root.mkdirs()
    workspaces.put("default", (None, SimpleWorkspace.create(root / "default")))


    override def list(): Seq[(String,Workspace)] = {
        val allWorkspaces = SimpleWorkspace.list(root)
        val allNames = allWorkspaces.map(_.name).toSet
        workspaces.synchronized {
            allWorkspaces.foreach { ws =>
                if (!workspaces.contains(ws.name)) {
                    workspaces.put(ws.name, (None, ws))
                }
            }

            val oldNames = workspaces.keySet -- allNames
            oldNames.foreach(workspaces.remove)
        }

        workspaces.map { case (id,ws) => id -> ws._2 }.toSeq
    }

    override def createWorkspace(id: String, name:String, clientId:Option[UUID]): (String,Workspace) = {
        val path = root / id
        if (SimpleWorkspace.exists(path)) {
            throw new IllegalArgumentException(s"Workspace '$id' already exists")
        }
        logger.info(s"Creating workspace '$id' for client '${clientId.getOrElse("")}'")
        val ws = SimpleWorkspace.create(path)

        workspaces.synchronized {
            workspaces.put(id, (clientId, ws))
        }

        id -> ws
    }

    override def deleteWorkspace(id: String): Unit = {
        val path = root / id
        if (!SimpleWorkspace.exists(path))
            throw new IllegalArgumentException(s"Workspace '$id' does not exists")

        logger.info(s"Deleting workspace '$id' at '$path'")
        workspaces.synchronized {
            workspaces.remove(id)
        }

        path.delete(true)
    }

    override def getWorkspace(id: String): (String,Workspace) = {
        val ws = workspaces.synchronized {
            workspaces.getOrElseUpdate(id, {
                val path = root / id
                if (!SimpleWorkspace.exists(path))
                    throw new IllegalArgumentException(s"Workspace '$id' does not exists")
                val ws = SimpleWorkspace.load(path)
                None -> ws
            })
        }

        id -> ws._2
    }

    override def removeClientWorkspaces(clientId: UUID): Unit = {
        val toBeRemoved = workspaces.synchronized {
            workspaces.filter(_._2._1.contains(clientId)).keys.toSeq
        }

        toBeRemoved.foreach { ws =>
            try {
                deleteWorkspace(ws)
            }
            catch {
                case NonFatal(ex) =>
                    logger.warn(s"Error removing client specific workspace '$ws':\n  ${reasons(ex)}")
            }
        }
    }
}
