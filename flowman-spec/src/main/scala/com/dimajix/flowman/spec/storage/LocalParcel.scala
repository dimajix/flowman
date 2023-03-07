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

import java.io.BufferedInputStream
import java.io.IOException

import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.common.tryWith
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.spec.ToSpec
import com.dimajix.flowman.storage.AbstractParcel


case class LocalParcel(override val name:String, override val root:File) extends AbstractParcel with ToSpec[ParcelSpec] {
    private val logger = LoggerFactory.getLogger(classOf[LocalParcel])

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

    @throws[IOException]
    override def clean() : Unit = {
        logger.info(s"Cleaning parcel '$name'")
        root.list().foreach(_.delete(true))
    }

    @throws[IOException]
    override def mkdir(fileName: Path) : Unit = {
        val newPath = zipSlipProtect(fileName.toString, root)
        newPath.mkdirs()
    }


    @throws[IOException]
    override def putFile(fileName: Path, content: Array[Byte]) : Unit = {
        val newPath = zipSlipProtect(fileName.toString, root)
        val parent = newPath.parent
        parent.mkdirs()
        // copy TarArchiveInputStream to Path newPath
        val out = newPath.create(true)
        try {
            IOUtils.write(content, out)
            out.close()
        }
        catch {
            case NonFatal(ex) =>
                newPath.delete()
                throw ex
        }
    }

    @throws[IOException]
    override def deleteFile(fileName: Path) : Unit = {
        val newPath = zipSlipProtect(fileName.toString, root)
        newPath.delete(true)
    }

    @throws[IOException]
    override def replace(targz: File): Unit = {
        if (!targz.isFile())
            throw new IOException(s"Source file '$targz' doesn't exists!")

        logger.info(s"Replacing parcel '$name' with new contents from '$targz")
        root.list().foreach(_.delete(true))
        decompressTarGzipFile(targz, root)
    }

    override def spec: ParcelSpec = {
        val spec = new LocalParcelSpec
        spec.name = name
        spec.root = root.toString
        spec
    }

    @throws[IOException]
    private def decompressTarGzipFile(source: File, target: File): Unit = {
        tryWith(source.open()) { fi =>
            tryWith(new BufferedInputStream(fi)) { bi =>
                tryWith(new GzipCompressorInputStream(bi)) { gzi =>
                    tryWith(new TarArchiveInputStream(gzi)) { ti =>
                        var entry: ArchiveEntry = ti.getNextEntry
                        while (entry != null) {
                            val newPath = zipSlipProtect(entry.getName, target)
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

    @throws[IOException]
    private def zipSlipProtect(entry: String, targetDir: File) : File = {
        val targetDirResolved = targetDir / entry
        val normalizePath = targetDirResolved.absolute
        if (!normalizePath.toString.startsWith(targetDir.toString))
            throw new IOException("Target location is not under workspace root: " + entry)
        normalizePath
    }
}


class LocalParcelSpec extends ParcelSpec {
    @JsonProperty(value="path", required=true) private[spec] var root:String = ""

    override def instantiate(root:File): LocalParcel = {
        LocalParcel(name, root / this.root)
    }
}
