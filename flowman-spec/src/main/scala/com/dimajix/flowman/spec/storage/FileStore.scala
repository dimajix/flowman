/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.storage

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchProjectException
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.storage.AbstractStore
import com.dimajix.flowman.storage.Store



case class FileStore(root:File) extends AbstractStore {
    private val logger = LoggerFactory.getLogger(classOf[FileStore])
    private val globPattern = "*/project.{yml,yaml}"

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
     * Retrieves a list of all projects. The returned projects only contain some fundamental information
     * like the projects name, its basedir and so on. The project itself (mappings, relations, targets etc)
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


class FileStoreSpec extends StoreSpec {
    @JsonProperty(value="location", required=true) private var root:String = ""

    override def instantiate(context:Context, properties:Option[Store.Properties] = None): Store = {
        val root = context.fs.file(context.evaluate(this.root))
        new FileStore(root)
    }
}
