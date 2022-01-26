/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.client.parcel

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Files
import java.net.URI

import scala.collection.JavaConverters._

import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.io.filefilter.DirectoryFileFilter
import org.apache.commons.io.filefilter.RegexFileFilter
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.ContentType
import org.apache.http.entity.FileEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.common.tryWith
import com.dimajix.flowman.client.Command


class DeployCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[DeployCommand])

    @Argument(index=0, usage = "specifies the parcel to delete", metaVar = "<workspace>/<parcel>", required = true)
    var fqParcel: String = ""
    @Argument(index=1, usage = "specifies the source to deploy. Can either be a directory or a tar.gz file", metaVar = "<source_dir>", required = true)
    var source: String = ""

    override def execute(httpClient:CloseableHttpClient, baseUri:URI) : Boolean = {
        val sourceFile = new File(source)
        if (!sourceFile.exists())
            throw new IllegalArgumentException(s"Source file '$source' does not exist")

        val parts = fqParcel.split('/')
        val (workspace, parcel) =
            if (parts.length == 1) {
                "default" -> parts(0)
            }
            else {
                parts(0) -> parts(1)
            }

        val isDirectory = sourceFile.isDirectory
        val targz = if (isDirectory) {
            val temp = Files.createTempFile("flowman-parcel", ".tgz").toFile
            createTarGzip(sourceFile, temp)
            temp
        }
        else {
            sourceFile
        }

        try {
            val request = new HttpPut(baseUri.resolve(s"workspace/$workspace/parcel/$parcel"))
            request.setEntity(new FileEntity(targz, ContentType.create("application/x-tar")))
            query(httpClient, request)
        }
        finally {
            targz.delete()
        }

        true
    }

    private def createTarGzip(inputDirectoryPath:File, outputFile:File) : Unit = {
        val files = FileUtils.listFiles(
            inputDirectoryPath,
            new RegexFileFilter("^(.*?)"),
            DirectoryFileFilter.DIRECTORY
        ).asScala


        tryWith(new FileOutputStream(outputFile)) { out =>
            tryWith(new BufferedOutputStream(out)) { out =>
                tryWith(new GzipCompressorOutputStream(out)) { out =>
                    tryWith(new TarArchiveOutputStream(out)) { tar =>
                        tar.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
                        tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);

                        files.foreach { file =>
                            val relativeFilePath = inputDirectoryPath.toURI.relativize(file.getAbsoluteFile.toURI).getPath
                            val tarEntry = new TarArchiveEntry(file, relativeFilePath)
                            tarEntry.setSize(file.length())

                            tar.putArchiveEntry(tarEntry)
                            tar.write(IOUtils.toByteArray(new FileInputStream(file)))
                            tar.closeArchiveEntry()
                        }
                    }
                }
            }
        }
    }
}
