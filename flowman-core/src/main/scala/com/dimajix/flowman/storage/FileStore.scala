/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.storage

import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.NoSuchProjectException
import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.model.Project


class FileStore(root:File) extends Store {
    private val logger = LoggerFactory.getLogger(classOf[FileStore])
    private val globPattern = new Path("*/project.{yml,yaml}")

    /**
     * Loads a project via its name (not its filename or directory)
     * @param name
     * @return
     */
    override def loadProject(name: String): Project = {
        root.glob(globPattern)
            .flatMap(file => loadProjectManifest(file).map((file, _)))
            .find(_._2.name == name)
            .map(fp => Project.read.file(fp._1))
            .getOrElse(throw new NoSuchProjectException(name))
    }

    /**
     * Stores a project inside this persistent storage
     * @param project
     */
    override def storeProject(project: Project): Unit = ???

    /**
     * Removes a project from this persistent storage
     * @param name
     */
    override def removeProject(name: String): Unit = ???

    /**
     * Retrieves a list of all projects. The returned projects only contain some fundamental information
     * like the projects's name, its basedir and so on. The project itself (mappings, relations, targets etc)
     * will not be loaded
     * @return
     */
    override def listProjects(): Seq[Project] = {
        root.glob(globPattern)
            .flatMap(loadProjectManifest)
    }

    private def loadProjectManifest(project:File) : Option[Project] = {
        try {
            Some(Project.read.manifest(project))
        } catch {
            case ex:Exception =>
                logger.warn(s"Cannot load project manifest '$project'", ex)
                None
        }
    }
}
