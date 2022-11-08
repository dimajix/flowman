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

package com.dimajix.flowman.fs

import java.io.InputStream
import java.io.OutputStream
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path


object File {
    def empty = HadoopFile(null, null)
}

/**
  * The File class represents a file on a Hadoop filesystem. It contains a path and a filesystem and provides
  * convenience methods for working with files.
  * @param fs
  * @param path
  */
abstract class File {
    override def toString: String = if (path != null) path.toString else ""

    def path : Path

    def uri : URI

    /**
      * Creates a new File object by attaching a child entry
      * @param sub
      * @return
      */
    def /(sub:String) : File

    /**
      * Returns the file name of the File
      * @return
      */
    def name : String

    /**
      * Returns the parent directory of the File
      * @return
      */
    def parent : File

    /**
      * Returns the absolute path
      * @return
      */
    def absolute : File

    /**
      * Returns the size of the file. Will throw an exception if the file does not exist
      * @return
      */
    def length : Long

    /**
      * Lists all directory entries. Will throw an exception if the File is not a directory
      * @return
      */
    def list() : Seq[File]

    def glob(pattern:String) : Seq[File]

    /**
      * Renamed the file to a different name
      * @param dst
      */
    def rename(dst:Path) : Unit

    /**
      * Copies the file to a different file. The relation file may reside on a different file system
      * @param dst
      * @param overwrite
      */
    def copy(dst:File, overwrite:Boolean) : Unit

    /**
      * Creates a file and returns the corresponding output stream. Intermediate directories will be created as required.
      * @param overwrite
      * @return
      */
    def create(overwrite:Boolean = false) : OutputStream

    def append() : OutputStream

    /**
      * Opens an existing file and returns the corresponding input stream
      * @return
      */
    def open() : InputStream

    /**
      * Deletes the file and/or directory
      * @param recursive
      */
    def delete(recursive:Boolean = false) : Unit

    /**
      * Returns true if the file exists. It can either be a file or a directory
      * @return
      */
    def exists() : Boolean

    def mkdirs() : Unit

    /**
      * Returns true if the file exists as a directory
      * @return
      */
    def isDirectory() : Boolean

    /**
      * Returns true if the file exists as a normal file
      * @return
      */
    def isFile() : Boolean

    /**
      * Returns true if the File is an absolute path
      * @return
      */
    def isAbsolute() : Boolean

    /**
      * Creates a new File instance with an additional suffix attached. This will not physically create the file
      * on the FileSystem, but will return a File which then can be used for creation
      * @param suffix
      * @return
      */
    def withSuffix(suffix:String) : File

    def withName(name:String) : File

}
