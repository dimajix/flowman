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

package com.dimajix.flowman.spec.storage

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.spec.ToSpec
import com.dimajix.flowman.storage.Parcel


case class LocalParcel(override val name:String, override val root:File) extends Parcel with ToSpec[ParcelSpec] {
    private val fileStore = FileStore(root)

    /**
     * Loads a project via its name (not its filename or directory)
     *
     * @param name
     * @return
     */
    override def loadProject(name: String): Project = fileStore.loadProject(name)

    /**
     * Retrieves a list of all projects. The returned projects only contain some fundamental information
     * like the projects name, its basedir and so on. The project itself (mappings, relations, targets etc)
     * will not be loaded
     *
     * @return
     */
    override def listProjects(): Seq[Project] = fileStore.listProjects()

    override def spec: ParcelSpec = {
        val spec = new LocalParcelSpec
        spec.name = name
        spec.root = root.toString
        spec
    }
}


class LocalParcelSpec extends ParcelSpec {
    @JsonProperty(value="path", required=true) private[spec] var root:String = ""

    override def instantiate(root:File): LocalParcel = {
        LocalParcel(name, root / this.root)
    }
}
