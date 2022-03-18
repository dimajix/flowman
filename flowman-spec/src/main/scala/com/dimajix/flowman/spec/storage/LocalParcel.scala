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

import java.io.BufferedInputStream
import java.io.IOException

import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.IOUtils

import com.dimajix.common.tryWith
import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.spec.ToSpec
import com.dimajix.flowman.storage.AbstractParcel


case class LocalParcel(override val name:String, override val root:File) extends AbstractParcel with ToSpec[ParcelSpec] {
    root.mkdirs()
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

    override def replace(targz: File): Unit = {
        if (!targz.isFile())
            throw new IOException(s"Source file '$targz' doesn't exists!")

        root.glob(new org.apache.hadoop.fs.Path("*")).foreach(_.delete(true))
        decompressTarGzipFile(targz, root)
    }

    override def spec: ParcelSpec = {
        val spec = new LocalParcelSpec
        spec.name = name
        spec.root = root.toString
        spec
    }

    def decompressTarGzipFile(source: File, target: File): Unit = {
        tryWith(source.open()) { fi =>
            tryWith(new BufferedInputStream(fi)) { bi =>
                tryWith(new GzipCompressorInputStream(bi)) { gzi =>
                    tryWith(new TarArchiveInputStream(gzi)) { ti =>
                        var entry: ArchiveEntry = ti.getNextEntry
                        while (entry != null) {
                            val newPath = zipSlipProtect(entry, target)
                            if (entry.isDirectory) {
                                newPath.mkdirs()
                            }
                            else { // check parent folder again
                                val parent = newPath.parent
                                parent.mkdirs()
                                // copy TarArchiveInputStream to Path newPath
                                val out = newPath.create(true)
                                try {
                                    IOUtils.copy(ti, out)
                                    out.close()
                                }
                                catch {
                                    case NonFatal(ex) =>
                                        newPath.delete()
                                        throw ex
                                }
                            }
                            entry = ti.getNextEntry
                        }
                    }
                }
            }
        }
    }

    private def zipSlipProtect(entry: ArchiveEntry, targetDir: File) : File = {
        val targetDirResolved = targetDir / entry.getName
        val normalizePath = targetDirResolved.absolute
        if (!normalizePath.toString.startsWith(targetDir.toString))
            throw new IOException("Bad entry: " + entry.getName)
        normalizePath
    }
}


class LocalParcelSpec extends ParcelSpec {
    @JsonProperty(value="path", required=true) private[spec] var root:String = ""

    override def instantiate(root:File): LocalParcel = {
        LocalParcel(name, root / this.root)
    }
}
