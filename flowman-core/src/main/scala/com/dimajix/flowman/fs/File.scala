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
import java.net.URL
import java.nio.file.FileSystemNotFoundException
import java.nio.file.NoSuchFileException
import java.nio.file.Paths
import java.util.Collections

import org.apache.hadoop.fs.Path

import com.dimajix.common.Resources
import com.dimajix.flowman.fs.FileSystem.SEPARATOR
import com.dimajix.flowman.fs.FileSystem.stripEndSlash
import com.dimajix.flowman.fs.FileSystem.stripProtocol
import com.dimajix.flowman.fs.FileSystem.stripSlash


object File {
    def empty = HadoopFile(null, null)

    /**
     * Static method for creating a [[File]] object for the local file system from a path
     * @param path
     * @return
     */
    def ofLocal(path: String): File = {
        val rawPath = stripEndSlash(stripProtocol(path))
        JavaFile(Paths.get(rawPath).normalize())
    }

    /**
     * Static method for creating a [[File]] object for the local file system from a Java file
     *
     * @param path
     * @return
     */
    def ofLocal(path: java.io.File): File = JavaFile(path.toPath.normalize())

    /**
     * Static method for creating a [[File]] object for the local file system from a [[URI]]
     *
     * @param path
     * @return
     */
    def ofLocal(path: URI): File = {
        if (path.getScheme == null) {
            val file = Paths.get(stripSlash(path.getPath))
            if (!file.isAbsolute) {
                JavaFile(file.normalize())
            }
            else {
                val uri = new URI("file", path.getUserInfo, path.getHost, path.getPort, path.getPath, path.getQuery, path.getFragment)
                JavaFile(Paths.get(uri).normalize())
            }
        }
        else {
            JavaFile(Paths.get(path).normalize())
        }
    }

    /**
     * Static method for creating a [[File]] object for a resource identified by the given path
     *
     * @param path
     * @return
     */
    def ofResource(path: String): File = {
        val url = Resources.getURL(path)
        if (url == null)
            throw new NoSuchFileException(s"Resource '$path' not found")
        ofResource(url.toURI)
    }

    /**
     * Static method for creating a [[File]] object for a resource identified by the given [[URL]]
     *
     * @param path
     * @return
     */
    def ofResource(url: URL): File = {
        ofResource(url.toURI)
    }

    /**
     * Static method for creating a [[File]] object for a resource identified by the given [[URI]]
     *
     * @param uri
     * @return
     */
    def ofResource(uri: URI): File = {
        if (uri.getScheme == "jar") {
            // Ensure JAR is opened as a file system
            try {
                java.nio.file.FileSystems.getFileSystem(uri)
            }
            catch {
                case _: FileSystemNotFoundException =>
                    java.nio.file.FileSystems.newFileSystem(uri, Collections.emptyMap[String, String]())
            }

            // Remove trailing "/", this is only present in Java 1.8
            val str = uri.toString
            val lastEx = str.lastIndexOf("!")
            val lastSep = str.lastIndexOf(SEPARATOR)
            if (lastSep == str.length - 1 && lastSep > lastEx + 1 && lastEx > 0) {
                JavaFile(Paths.get(new URI(str.dropRight(1))))
            }
            else {
                JavaFile(Paths.get(uri))
            }
        }
        else {
            JavaFile(Paths.get(uri))
        }
    }
}


/**
  * The File class represents a file on a Hadoop filesystem. It contains a path and a filesystem and provides
  * convenience methods for working with files.
  * @param fs
  * @param path
  */
abstract class File {
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
