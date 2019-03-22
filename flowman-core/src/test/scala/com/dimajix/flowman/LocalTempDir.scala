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

package com.dimajix.flowman

import java.io.File
import java.io.IOException
import java.util.UUID

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite


trait LocalTempDir extends BeforeAndAfterAll {  this:Suite =>
    var tempDir: File = _

    override def beforeAll() : Unit = {
        tempDir = createTempDir()
    }
    override def afterAll() : Unit = {
        if (tempDir != null) {
            deleteTempDir(tempDir)
            tempDir = null
        }
    }

    /**
      * Create a directory inside the given parent directory.
      * The directory is guaranteed to be newly created, and is not marked for automatic
      * deletion.
      */
    private def createDirectory(root: String): File = {
        var attempts = 0
        val maxAttempts = 10
        var dir: File = null
        while (dir == null) {
            attempts += 1
            if (attempts > maxAttempts) {
                throw new IOException(
                    s"Failed to create a temp directory (under ${root}) after ${maxAttempts}")
            }
            try {
                dir = new File(root, "spark-" + UUID.randomUUID.toString)
                if (dir.exists() || !dir.mkdirs()) {
                    dir = null
                }
            } catch { case e: SecurityException => dir = null; }
        }

        dir
    }

    /**
      * Create a temporary directory inside the given parent directory.
      * The directory will be automatically deleted when the VM shuts down.
      */
    private def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
        val dir = createDirectory(root)
        dir
    }

    private def deleteTempDir(dir:File) : Unit = {
        deleteRecursively(dir)
    }

    private def deleteRecursively(file: File): Unit = {
        if (file.isDirectory)
            file.listFiles.foreach(deleteRecursively)
        if (file.exists) {
            // Silently eat up all exceptions
            try {
                file.delete()
            }
            catch {
                case ex:IOException => 
            }
        }
    }
}
