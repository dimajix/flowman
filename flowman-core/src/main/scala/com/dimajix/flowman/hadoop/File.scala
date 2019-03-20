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

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils


object File {
    def empty = new File(null, null)
    def apply(conf:Configuration, path:Path) : File  = {
        File(path.getFileSystem(conf), path)
    }
    def apply(conf:Configuration, path:String) : File = {
        apply(conf, new Path(path))
    }
}

/**
  * The File class represents a file on a Hadoop filesystem. It contains a path and a filesystem and provides
  * convenience methods for working with files.
  * @param fs
  * @param path
  */
case class File(fs:org.apache.hadoop.fs.FileSystem, path:Path) {
    override def toString: String = if (path != null) path.toString else ""

    /**
      * Creates a new File object by attaching a child entry
      * @param sub
      * @return
      */
    def /(sub:String) : File = {
        File(fs, new Path(path, sub))
    }

    /**
      * Returns the file name of the File
      * @return
      */
    def filename : String = {
        path.getName
    }

    /**
      * Returns the parent directory of the File
      * @return
      */
    def parent : File = {
        File(fs, path.getParent)
    }

    /**
      * Returns the absolute path
      * @return
      */
    def absolute : File = {
        File(fs, path.makeQualified(fs.getUri, fs.getWorkingDirectory))
    }

    /**
      * Returns the size of the file. Will throw an exception if the file does not exist
      * @return
      */
    def length : Long = {
        fs.getFileStatus(path).getLen
    }

    /**
      * Lists all directory entries. Will throw an exception if the File is not a directory
      * @return
      */
    def list() : Seq[File] = {
        if (!isDirectory())
            throw new IOException(s"File '$path' is not a directory - cannot list files")
        fs.listStatus(path)
            .map(item => (item.getPath.toString, File(fs, item.getPath)))
            .sortBy(_._1)
            .map(_._2)
    }

    /**
      * Renames the file to a different name. The destination has to be on the same FileSystem, otherwise an
      * exception will be thrown
      * @param dst
      */
    def rename(dst:File) : Unit  = {
        if (!dst.fs.eq(fs)) {
            throw new IOException(s"Target of rename needs to be on the same filesystem")
        }
        rename(dst.path)
    }

    /**
      * Renamed the file to a different name
      * @param dst
      */
    def rename(dst:Path) : Unit  = {
        if (fs.exists(dst) && !fs.delete(dst, false)) {
            throw new IOException(s"Cannot rename '$path' to '$dst', because '$dst' already exists")
        }

        if (!fs.rename(path, dst)) {
            throw new IOException(s"Cannot rename '$path' to '$dst'")
        }
    }

    /**
      * Copies the file to a different file. The relation file may reside on a different file system
      * @param dst
      * @param overwrite
      */
    def copy(dst:File, overwrite:Boolean) : Unit = {
        if (!overwrite && dst.isFile())
            throw new IOException("Target $dst already exists")

        // Append file name if relation is a directory
        val dstFile = if (dst.isDirectory())
            dst / path.getName
        else
            dst

        // Perform copy
        if (dstFile.fs.isInstanceOf[LocalFileSystem])
            copyToLocal(dstFile, overwrite)
        else
            copyToRemote(dstFile, overwrite)
    }

    /**
      * Creates a file and returns the correspondiong output stream
      * @param overwrite
      * @return
      */
    def create(overwrite:Boolean = false) : FSDataOutputStream = {
        fs.create(path, overwrite)
    }

    /**
      * Opens an existing file and returns the corresponding input stream
      * @return
      */
    def open() : FSDataInputStream = {
        fs.open(path)
    }

    /**
      * Deletes the file and/or directory
      * @param recursive
      */
    def delete(recursive:Boolean = false) : Unit = {
        if (fs.exists(path) && !fs.delete(path, recursive)) {
            throw new IOException(s"Cannot delete '$path'")
        }
    }

    /**
      * Returns true if the file exists. It can either be a file or a directory
      * @return
      */
    def exists() : Boolean = {
        fs.exists(path)
    }

    /**
      * Returns true if the file exists as a directory
      * @return
      */
    def isDirectory() : Boolean = {
        fs.isDirectory(path)
    }

    /**
      * Returns true if the file exists as a normal file
      * @return
      */
    def isFile() : Boolean = {
        fs.isFile(path)
    }

    /**
      * Returns true if the File is an absolute path
      * @return
      */
    def isAbsolute() : Boolean = {
        path.isAbsolute
    }

    /**
      * Creates a new File instance with an additional suffix attached. This will not physically create the file
      * on the FileSystem, but will return a File which then can be used for creation
      * @param suffix
      * @return
      */
    def withSuffix(suffix:String) : File = {
        File(fs, path.suffix(suffix))
    }

    def withName(name:String) : File = {
        File(fs, new Path(path.getParent, name))
    }

    private def copyToLocal(dst:File, overwrite:Boolean) : Unit = {
        val output = dst.create(overwrite)
        try {
            val input = open()
            try {
                IOUtils.copyBytes(input, output, 16384, true)
            }
            finally {
                input.close()
            }
        }
        catch {
            case ex:Throwable =>
                output.close()
                dst.delete()
                throw ex
        }
    }

    private def copyToRemote(dst:File, overwrite:Boolean) : Unit = {
        val tmp = dst.withSuffix("._COPYING_")
        val output = tmp.create(overwrite)
        try {
            val input = open()
            try {
                IOUtils.copyBytes(input, output, 16384, true)
                tmp.rename(dst)
            }
            finally {
                input.close()
            }
        }
        catch {
            case ex:Throwable =>
                output.close()
                tmp.delete()
                throw ex
        }
    }
}
