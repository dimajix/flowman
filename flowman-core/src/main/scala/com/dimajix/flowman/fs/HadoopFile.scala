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

import java.io.FileNotFoundException
import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils


/**
  * The File class represents a file on a Hadoop filesystem. It contains a path and a filesystem and provides
  * convenience methods for working with files.
  * @param fs
  * @param path
  */
case class HadoopFile(fs:org.apache.hadoop.fs.FileSystem, path:Path) extends File {
    override def uri : URI = path.toUri

    /**
      * Creates a new File object by attaching a child entry
      * @param sub
      * @return
      */
    override def /(sub:String) : File = {
        val rel = new Path(new URI(sub))
        if (rel.isAbsolute)
            HadoopFile(fs, rel)
        else
            HadoopFile(fs, new Path(path, sub))
    }

    /**
     * Returns the file name of the File
     *
     * @return
     */
    override def name: String = {
        path.getName
    }

    /**
      * Returns the parent directory of the File
      * @return
      */
    override def parent : File = {
        val p = path.getParent
        if (p == null) {
            this
        }
        else if (p.getName.isEmpty) {
            HadoopFile(fs, p)
        }
        else {
            val uri = new URI(p.toUri.toString + "/")
            HadoopFile(fs, new Path(uri))
        }
    }

    /**
      * Returns the absolute path
      * @return
      */
    def absolute : File = {
        HadoopFile(fs, path.makeQualified(fs.getUri, fs.getWorkingDirectory))
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
            .map(item => (item.getPath.toString, HadoopFile(fs, item.getPath)))
            .sortBy(_._1)
            .map(_._2)
    }

    def glob(pattern:String) : Seq[File] = {
        if (!isDirectory())
            throw new IOException(s"File '$path' is not a directory - cannot list files")
        fs.globStatus(new Path(path, pattern))
            .map(item => (item.getPath.toString, HadoopFile(fs, item.getPath)))
            .sortBy(_._1)
            .map(_._2)
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
        dstFile match {
            case HadoopFile(fs, _) if fs.isInstanceOf[LocalFileSystem] =>
                copyToLocal(dstFile, overwrite)
            case _ =>
                copyToRemote(dstFile, overwrite)
        }
    }

    /**
      * Creates a file and returns the corresponding output stream. Intermediate directories will be created as required.
      * @param overwrite
      * @return
      */
    def create(overwrite:Boolean = false) : FSDataOutputStream = {
        fs.create(path, overwrite)
    }

    def append(): FSDataOutputStream = {
        fs.append(path)
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

    def mkdirs() : Unit = {
        if (!fs.mkdirs(path))
            throw new IOException(s"Cannot create directory '$path'")
    }

    /**
      * Returns true if the file exists as a directory
      * @return
      */
    def isDirectory() : Boolean = {
      try {
         fs.getFileStatus(path).isDirectory
      }
      catch {
        case _: FileNotFoundException => false
      }
    }

    /**
      * Returns true if the file exists as a normal file
      * @return
      */
    def isFile() : Boolean = {
      try {
        fs.getFileStatus(path).isFile
      }
      catch {
        case _: FileNotFoundException => false
      }
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
        HadoopFile(fs, path.suffix(suffix))
    }

    def withName(name:String) : File = {
        HadoopFile(fs, new Path(path.getParent, name))
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
                tmp.rename(dst.path)
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
