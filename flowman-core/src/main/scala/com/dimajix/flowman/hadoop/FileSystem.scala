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

package com.dimajix.flowman.hadoop

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path


/**
  * This is a super thin wrapper around Hadoop FileSystems which is used to create Flowman File instances
  * @param conf
  */
case class FileSystem(conf:Configuration) {
    private val localFs = org.apache.hadoop.fs.FileSystem.getLocal(conf)

    def file(path:Path) : File = File(path.getFileSystem(conf), path)
    def file(path:String) : File = file(new Path(path))
    def file(path:URI) : File = file(new Path(path))

    def local(path:Path) : File = File(localFs, path)
    def local(path:String) : File = local(new Path(path))
    def local(path:java.io.File) : File = local(new Path(path.toString))
    def local(path:URI) : File = local(new Path(path))
}
