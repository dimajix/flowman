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

package com.dimajix.flowman.spec.storage

import scala.util.Try

import com.dimajix.flowman.fs.File
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.storage.AbstractWorkspace
import com.dimajix.flowman.storage.Parcel


object SimpleWorkspace {
    def load(root:File) : SimpleWorkspace = {
        if (!root.isDirectory())
            throw new IllegalArgumentException(s"Specified workspace root directory does not exist: $root")
        val parcel = LocalParcel(root.name, root)
        SimpleWorkspace(parcel)
    }

    def create(root:File): SimpleWorkspace = {
        val parcel = LocalParcel(root.name, root)
        SimpleWorkspace(parcel)
    }

    def list(root: File): Seq[SimpleWorkspace] = {
        val globPattern = "*"
        root.glob(globPattern)
            .flatMap(file => Try(load(file)).toOption)
    }

    def exists(file: File): Boolean = {
        file.isDirectory()
    }
}

case class SimpleWorkspace(parcel:Parcel) extends AbstractWorkspace {
    override def root: File = parcel.root

    override def name : String = parcel.name

    /**
     * Loads a project via its name (not its filename or directory)
     *
     * @param name
     * @return
     */
    override def loadProject(name: String): Project = {
        parcel.loadProject(name)
    }

    /**
     * Retrieves a list of all projects. The returned projects only contain some fundamental information
     * like the projects name, its basedir and so on. The project itself (mappings, relations, targets etc)
     * will not be loaded
     *
     * @return
     */
    override def listProjects(): Seq[Project] = parcels.flatMap(_.listProjects())

    override def parcels: Seq[Parcel] = Seq(parcel)

    override def addParcel(parcel: Parcel): Unit = {
        ???
    }

    override def removeParcel(parcel: String): Unit = {
        ???
    }

    override def clean() : Unit = {
        parcel.clean()
    }
}
