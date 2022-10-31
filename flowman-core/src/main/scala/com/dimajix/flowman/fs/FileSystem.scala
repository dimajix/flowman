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

package com.dimajix.flowman.fs

import java.net.URI
import java.nio.file.FileSystemNotFoundException
import java.nio.file.Paths
import java.util.Collections

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import com.dimajix.common.Resources


/**
  * This is a super thin wrapper around Hadoop FileSystems which is used to create Flowman File instances
  * @param conf
  */
case class FileSystem(conf:Configuration) {
    def file(path:Path) : File = {
        val fs = path.getFileSystem(conf)
        HadoopFile(fs, path)
    }
    def file(path:String) : File = file(new Path(path))
    def file(path:URI) : File = file(new Path(path))

    def local(path:Path) : File = local(path.toUri)
    def local(path:String) : File = JavaFile(Paths.get(path))
    def local(path:java.io.File) : File = JavaFile(path.toPath)
    def local(path:URI) : File = JavaFile(Paths.get(path))

    def resource(path:String) : File = {
        val uri = Resources.getURL(path).toURI
        if (uri.getScheme == "jar") {
            try {
                java.nio.file.FileSystems.getFileSystem(uri)
            }
            catch {
                case _: FileSystemNotFoundException =>
                    java.nio.file.FileSystems.newFileSystem(uri, Collections.emptyMap[String, String]())
            }
            JavaFile(Paths.get(uri))
        }
        else {
            JavaFile(Paths.get(uri))
        }
    }
}
