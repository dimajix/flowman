/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

import java.io.FileNotFoundException

import org.apache.hadoop.fs.Path


object FileUtils {
    /**
     * Returns true if the path refers to a successfully written Hadoop/Spark job. This is the case if either the
     * location refers to an existing file or if the location refers to a directory which contains a "_SUCCESS" file.
     * @param fs
     * @param location
     * @return
     */
    def isValidFileData(fs:org.apache.hadoop.fs.FileSystem, location:Path): Boolean = {
        try {
            val status = fs.getFileStatus(location)
            if (status.isFile) {
                true
            }
            else {
                val success = new Path(location, "_SUCCESS")
                fs.getFileStatus(success).isFile
            }
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
        try {
            val status = fs.getFileStatus(location)
            if (status.isFile) {
                true
            }
            else {
                fs.listStatus(location).nonEmpty
            }
        }
        catch {
            case _: FileNotFoundException => false
        }
    }

    /**
     * Returns true if the path refers to a successfully written Hadoop job. This is the case if either the location
     * refers to an existing file or if the location refers to a directory which contains a "_SUCCESS" file.
     * @param file
     * @return
     */
    def isValidData(file:File) : Boolean = {
        isValidFileData(file.fs, file.path)
    }
}
