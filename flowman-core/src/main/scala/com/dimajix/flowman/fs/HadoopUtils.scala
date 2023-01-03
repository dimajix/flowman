/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory


class HadoopUtils

object HadoopUtils {
    private val logger = LoggerFactory.getLogger(classOf[HadoopUtils])

    def isGlobbingPattern(pattern: Path) : Boolean = {
        isGlobbingPattern(pattern.toString)
    }

    def isGlobbingPattern(pattern: String): Boolean = {
        pattern.exists("{}[]*?\\".toSet.contains)
    }

    /**
     * Returns true if the path refers to a successfully written Hadoop/Spark job. This is the case if either the
     * location refers to an existing file or if the location refers to a directory which contains a "_SUCCESS" file.
     * @param fs
     * @param location
     * @return
     */
    def isValidFileData(fs:org.apache.hadoop.fs.FileSystem, location:Path, requireSuccessFile:Boolean = true): Boolean = {
        try {
            val status = fs.getFileStatus(location)
            if (status.isFile) {
                true
            }
            else {
                if (requireSuccessFile) {
                    val success = new Path(location, "_SUCCESS")
                    fs.getFileStatus(success).isFile
                }
                else {
                    fs.listStatus(location).nonEmpty
                }
            }
        }
        catch {
            case _: FileNotFoundException => false
        }
    }

    def isValidStreamData(fs:org.apache.hadoop.fs.FileSystem, location:Path) : Boolean = {
        try {
            val meta = new Path(location, "_spark_metadata") // For streaming queries
            fs.getFileStatus(meta).isDirectory
        }
        catch {
            case _: FileNotFoundException => false
        }
    }

    /**
     * Returns true if the path refers to a successfully written Hadoop/Spark job. This is the case if either the
     * location refers to an existing file or if the location refers to a directory. Note that Hive tables do not
     * neccessarily contain "_SUCCESS" files
     * @param fs
     * @param location
     * @return
     */
    def isValidHiveData(fs:org.apache.hadoop.fs.FileSystem, location:Path): Boolean = {
        isValidFileData(fs, location, false)
    }

    def isPartitionedData(fs:org.apache.hadoop.fs.FileSystem, location:Path) : Boolean = {
        if (fs.exists(location) && fs.getFileStatus(location).isDirectory) {
            val iter = fs.listLocatedStatus(location)
            var success = false
            while(!success && iter.hasNext) {
                val status = iter.next()
                if (status.isDirectory && status.getPath.getName.contains("="))
                    success = true
            }
            success
        }
        else {
            false
        }
    }

    /**
     * Truncates a path, which means that wither all contents of a directory are removed (if the path points to a
     * directory) or that the file itself is deleted (if the path points to a file). The directory in the first case
     * will be kept in tact.
     * @param fs
     * @param location
     */
    def truncateLocation(fs:org.apache.hadoop.fs.FileSystem, location:Path): Unit = {
        java.lang.System.gc() // Release open file handles on Windows
        try {
            val status = fs.getFileStatus(location)
            if (status.isDirectory()) {
                logger.info(s"Deleting all files in directory '$location'")
                java.lang.System.gc() // Release open file handles on Windows
                fs.listStatus(location).foreach { f =>
                    doDelete(fs, f.getPath, true)
                }
            }
            else if (status.isFile()) {
                logger.info(s"Deleting single file '$location'")
                doDelete(fs, location, false)
            }
        } catch {
            case _:FileNotFoundException =>
        }
    }

    /**
     * Creates a new directory, if it does not already exist
     * @param fs
     * @param location
     */
    def createLocation(fs:org.apache.hadoop.fs.FileSystem, location:Path) : Unit = {
        if (!fs.exists(location)) {
            logger.info(s"Creating directory '$location'")
            fs.mkdirs(location)
        }
    }

    /**
     * Completely removes a directory and all subdirectories
     * @param fs
     * @param location
     */
    def deleteLocation(fs:org.apache.hadoop.fs.FileSystem, location:Path): Unit = {
        java.lang.System.gc() // Release open file handles on Windows
        if (fs.exists(location)) {
            logger.info(s"Deleting file or directory '$location'")
            doDelete(fs, location, true)
        }
    }

    private def doDelete(fs:org.apache.hadoop.fs.FileSystem, location:Path, recursive:Boolean) : Unit = {
        if (!fs.delete(location, recursive)) {
            logger.warn(s"Failed to delete file or directory '$location'")
        }
    }
}
