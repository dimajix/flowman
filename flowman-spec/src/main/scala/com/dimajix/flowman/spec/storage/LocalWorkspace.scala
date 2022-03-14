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

import scala.collection.mutable
import scala.util.Try

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path

import com.dimajix.flowman.execution.NoSuchProjectException
import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.ToSpec
import com.dimajix.flowman.storage.AbstractWorkspace
import com.dimajix.flowman.storage.Parcel
import com.dimajix.flowman.storage.Workspace


object LocalWorkspace {
    def load(file:File) : LocalWorkspace = new LocalWorkspace(file)

    def list(root:File) : Seq[Workspace] = {
        val globPattern = new Path("*/.flowman-workspace.yaml")
        root.glob(globPattern)
            .flatMap(file => Try(load(file.parent)).toOption)
    }

    def exists(file:File) : Boolean = {
        file.isDirectory() && (file / ".flowman-workspace.yaml").isFile()
    }
}

case class LocalWorkspace(override val root:File) extends AbstractWorkspace {
    private val _parcels = mutable.ListBuffer[Parcel]()

    root.mkdirs()
    val file = root / ".flowman-workspace.yaml"
    if (!file.exists()) {
        writeWorkspaceFile()
    }
    else {
        val spec = ObjectMapper.read[ParcelWorkspaceSpec](file)
        spec.parcels.foreach(p => _parcels.append(p.instantiate(root)))
    }

    override def name : String = root.path.getName

    /**
     * Loads a project via its name (not its filename or directory)
     *
     * @param name
     * @return
     */
    override def loadProject(name: String): Project = {
        parcels.find(_.listProjects().exists(_.name == name))
            .map(_.loadProject(name))
            .getOrElse(throw new NoSuchProjectException(name))
    }

    /**
     * Retrieves a list of all projects. The returned projects only contain some fundamental information
     * like the projects name, its basedir and so on. The project itself (mappings, relations, targets etc)
     * will not be loaded
     *
     * @return
     */
    override def listProjects(): Seq[Project] = parcels.flatMap(_.listProjects())

    override def parcels: Seq[Parcel] = _parcels.toSeq

    override def addParcel(parcel: Parcel): Unit = {
        _parcels.synchronized {
            if (_parcels.exists(_.name == parcel.name)) {
                throw new IllegalArgumentException(s"A Parcel with name ${parcel.name} is already part of the workspace.")
            }
            _parcels.append(parcel)
        }
        writeWorkspaceFile()
    }

    override def removeParcel(parcel: String): Unit = {
        _parcels.synchronized {
            val idx = _parcels.indexWhere(_.name == parcel)
            if (idx < 0) {
                throw new IllegalArgumentException(s"No parcel with name $parcel is part of the workspace")
            }
            _parcels.remove(idx)
        }
        writeWorkspaceFile()
    }

    private def writeWorkspaceFile() : Unit = {
        val spec = new ParcelWorkspaceSpec
        spec.parcels = _parcels.map(_.asInstanceOf[ToSpec[ParcelSpec]].spec)

        val file = root / ".flowman-workspace.yaml"
        val out = file.create(overwrite = true)
        try {
            ObjectMapper.write(out, spec)
        }
        finally {
            out.close()
        }
    }
}


class ParcelWorkspaceSpec {
    @JsonProperty(value="parcels", required = true) var parcels: Seq[ParcelSpec] = Seq()
}
