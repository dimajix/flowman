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

import org.slf4j.LoggerFactory

import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.spec.storage.LocalWorkspace
import com.dimajix.flowman.storage.Workspace


class WorkspaceManager(root:File) {
    private val logger = LoggerFactory.getLogger(classOf[WorkspaceManager])

    logger.info(s"Initialized WorkspaceManager at '$root'")

    // Create workspace are + default workspace
    root.mkdirs()
    new LocalWorkspace(root / "default")

    def list() : Seq[String] = {
        LocalWorkspace.list(root)
            .collect { case ws:LocalWorkspace => ws.root.path.getName }
    }

    def createWorkspace(name:String) : Workspace = ???
    def deleteWorkspace(name:String) : Unit = ???
    def getWorkspace(name:String) : Workspace = ???
}
