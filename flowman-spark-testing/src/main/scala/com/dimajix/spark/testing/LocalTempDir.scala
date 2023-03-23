/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.spark.testing

import java.io.File
import java.io.IOException
import java.util.UUID

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite


trait LocalTempDir extends Logging {  this:Suite =>
    private var _tempDir : Option[File] = None
    def tempDir: File = _tempDir.getOrElse(throw new IllegalStateException("No temp dir available"))

    override def beforeAll() : Unit = {
        super.beforeAll()

        _tempDir = createTempDir()
    }
    override def afterAll() : Unit = {
        _tempDir.foreach { tempDir =>
            deleteTempDir(tempDir)
            _tempDir = None
        }

        super.afterAll()
    }

    def withTempDir(f: File => Unit): Unit = {
        val dir = createTempDir().map(_.getCanonicalFile).getOrElse(throw new IllegalStateException("Cannot create temp dir"))
        try f(dir) finally {
            deleteRecursively(dir)
        }
    }

    /**
      * Create a directory inside the given parent directory.
      * The directory is guaranteed to be newly created, and is not marked for automatic
      * deletion.
      */
    private def createDirectory(root: String): Option[File] = {
        var attempts = 0
        val maxAttempts = 10
        var dir: Option[File] = None
        while (dir.isEmpty) {
            attempts += 1
            if (attempts > maxAttempts) {
                throw new IOException(
                    s"Failed to create a temp directory (under ${root}) after ${maxAttempts}")
            }
            try {
                val newdir = new File(root, "spark-" + UUID.randomUUID.toString)
                if (newdir.exists() || !newdir.mkdirs()) {
                    dir = None
                }
                else {
                    dir = Some(newdir)
                }
            } catch { case e: SecurityException => dir = None }
        }

        dir
    }

    /**
      * Create a temporary directory inside the given parent directory.
      * The directory will be automatically deleted when the VM shuts down.
      */
    private def createTempDir(root: String = System.getProperty("java.io.tmpdir")): Option[File] = {
        createDirectory(root)
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
