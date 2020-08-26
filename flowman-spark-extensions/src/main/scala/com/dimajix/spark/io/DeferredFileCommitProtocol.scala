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

package com.dimajix.spark.io

import java.io.FileNotFoundException

import scala.collection.mutable

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.JobContext
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol


class DeferredFileCommitProtocol(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false
) extends SQLHadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite) {
    @transient private val filesToBeDeleted: mutable.ListBuffer[(FileSystem,Path)] = mutable.ListBuffer()
    @transient private val directoriesToBeDeleted: mutable.ListBuffer[(FileSystem,Path)] = mutable.ListBuffer()

    override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
        // First remove all files
        filesToBeDeleted.foreach { case(fs,path) =>
            try {
                fs.delete(path, false)
            }
            catch {
                // Ignore if file does not exist
                case _:FileNotFoundException =>
            }
        }
        // Now remove all empty directories
        directoriesToBeDeleted.foreach { case(fs,path) =>
            try {
                if (fs.listStatus(path).isEmpty) {
                    fs.delete(path, false)
                }
            }
            catch {
                // Ignore if file does not exist
                case _:FileNotFoundException =>
            }
        }

        super.commitJob(jobContext, taskCommits)
    }

    override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
        def collectFilesRecursively(fs: FileSystem, path: Path) : Unit = {
            try {
                val iter = fs.listFiles(path, recursive)
                while(iter.hasNext) {
                    val status = iter.next()
                    val p = status.getPath
                    if (status.isDirectory) {
                        collectFilesRecursively(fs, p)
                    }
                    else {
                        filesToBeDeleted.append((fs, p))
                    }
                }
                // Add path itself, after whole content was added
                directoriesToBeDeleted.append((fs,path))
            }
            catch {
                // Ignore if file does not exist
                case _:FileNotFoundException =>
            }

        }

        if (!recursive) {
            filesToBeDeleted.append((fs, path))
        }
        else {
            collectFilesRecursively(fs, path)
        }

        true
    }
}
