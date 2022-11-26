/*
 * Copyright 2022 Kaya Kupferschmidt
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

import java.net.URI
import java.nio.file.Paths
import java.util.Collections

import org.apache.hadoop.fs
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.Resources
import com.dimajix.spark.testing.LocalTempDir


class JavaFileTest extends AnyFlatSpec with Matchers with LocalTempDir {
    "The JavaFile" should "work" in {
        val prefix = if (FileSystem.WINDOWS) "file:/" else "file:"
        val dir = JavaFile(tempDir.toPath)
        dir.uri should be (tempDir.toURI)
        dir.path should be (new fs.Path(tempDir.toURI))
        dir.toString should be (prefix + tempDir.toString.replace('\\','/'))
        dir.exists() should be (true)
        dir.isFile() should be (false)
        dir.isDirectory() should be (true)
        dir.isAbsolute() should be (true)

        if (!FileSystem.WINDOWS) {
            (dir / dir.toString) should be (dir)
        }

        val file = dir / "lala"
        file.name should be ("lala")
        file.uri should be (tempDir.toURI.resolve("lala"))
        file.path should be (new Path(tempDir.toURI.resolve("lala")))
        file.toString should be (new java.io.File(tempDir, "lala").toURI.toString)
        file.exists() should be(false)
        file.isFile() should be(false)
        file.isDirectory() should be(false)
        file.isAbsolute() should be (true)
        file.parent should be(dir)
        file.name should be("lala")
        file.withName("lolo") should be(dir / "lolo")

        val file2 = dir / "lala/"
        file2.toString should be(tempDir.toURI.toString + "lala")
        file2.uri should be(tempDir.toURI.resolve("lala"))
        file2.path should be(new Path(tempDir.toURI.resolve("lala")))
        file2.path should be(new Path(tempDir.toURI.toString, "lala"))
        file2.exists() should be(false)
        file2.isFile() should be(false)
        file2.isDirectory() should be(false)
        file2.isAbsolute() should be(true)
        file2.parent should be(dir)
        file2.name should be("lala")
        file2.withName("lolo") should be(dir / "lolo")
    }

    it should "work at root level" in {
        val dir = JavaFile(tempDir.toPath.getRoot)
        dir.parent should be (null)
        dir.name should be ("")
        dir.uri should be(new URI("file:/"))
        dir.path should be(new fs.Path("file:/"))
        dir.toString should be ("file:/")
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isDirectory() should be(true)
        dir.isAbsolute() should be(true)

        val file = dir / "lala"
        file.parent should be (dir)
        file.name should be ("lala")
        file.uri should be(new URI("file:/lala"))
        file.path should be(new fs.Path("file:/lala"))
        file.exists() should be(false)
        file.isFile() should be(false)
        file.isDirectory() should be(false)
        file.isAbsolute() should be(true)
        file.parent should be(dir)
        file.withName("lolo") should be(dir / "lolo")
    }

    it should "support creating entries" in {
        val tmp = JavaFile(tempDir.toPath)
        val name = "lala-" + System.currentTimeMillis().toString + ".tmp"
        val file = tmp / name
        file.uri should be (tempDir.toURI.resolve(name))
        file.path should be (new Path(tempDir.toURI.resolve(name)))
        file.path should be (new Path(tempDir.toURI.toString ,name))
        file.name should be (name)
        file.exists() should be(false)
        file.isFile() should be(false)
        file.isDirectory() should be(false)

        file.create().close()
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isDirectory() should be(false)

        file.delete(false)
        file.exists() should be(false)
        file.isFile() should be(false)
        file.isDirectory() should be(false)
    }

    it should "support resources somewhere" in {
        val res = Resources.getURL("com/dimajix/flowman/flowman.properties")
        val file = JavaFile(Paths.get(res.toURI))
        file.name should be ("flowman.properties")
        file.uri should be (res.toURI)
        file.path.toUri should be (res.toURI)
        file.path should be (new Path(res.toURI))
        file.exists() should be (true)
        file.isFile() should be (true)
        file.isAbsolute() should be (true)
        file.isDirectory() should be(false)

        val res1 = Resources.getURL("com/dimajix/flowman")
        val dir1 = JavaFile(Paths.get(res1.toURI))
        dir1.name should be ("flowman")
        //dir1.uri should be (res1.toURI)
        dir1.path.toString should be (res1.toURI.toString + "/")
        dir1.exists() should be(true)
        dir1.isFile() should be(false)
        dir1.isAbsolute() should be(true)
        dir1.isDirectory() should be(true)

        val res2 = Resources.getURL("com/dimajix/flowman/")
        val dir2 = JavaFile(Paths.get(res2.toURI))
        dir2.name should be ("flowman")
        dir2.uri should be (res2.toURI)
        dir2.path should be(new Path(res2.toURI))
        dir2.path.toUri should be(res2.toURI)
        dir2.exists() should be(true)
        dir2.isFile() should be(false)
        dir2.isAbsolute() should be(true)
        dir2.isDirectory() should be(true)
    }

    it should "support resources in JARs" in {
        val res = Resources.getURL("org/apache/spark/SparkContext.class")
        val xyz = java.nio.file.FileSystems.newFileSystem(res.toURI, Collections.emptyMap[String,String]())
        val file = JavaFile(Paths.get(res.toURI))
        file.uri should be (res.toURI)
        file.path.toUri should be (res.toURI)
        file.path should be (new Path(res.toURI))
        file.name should be ("SparkContext.class")
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isAbsolute() should be(true)
        file.isDirectory() should be(false)

        val res1 = Resources.getURL("org/apache/spark")
        val dir1 = JavaFile(Paths.get(res1.toURI))
        dir1.uri should be (res1.toURI)
        dir1.path should be (new Path(res1.toURI))
        dir1.name should be ("spark")
        dir1.exists() should be(true)
        dir1.isFile() should be(false)
        dir1.isAbsolute() should be(true)
        dir1.isDirectory() should be(true)

        val res2 = Resources.getURL("org/apache/spark/")
        val dir2 = JavaFile(Paths.get(res2.toURI))
        //dir2.uri should be(res2.toURI)
        //dir2.path should be(new Path(res2.toURI))
        dir2.name should be("spark")
        dir2.exists() should be(true)
        dir2.isFile() should be(false)
        dir2.isAbsolute() should be(true)
        dir2.isDirectory() should be(true)

        xyz.close()
    }
}
