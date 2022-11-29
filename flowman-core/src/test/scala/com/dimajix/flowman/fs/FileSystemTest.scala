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

import java.net.URI
import java.nio.file.NoSuchFileException

import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.Resources
import com.dimajix.flowman.fs.FileSystem.WINDOWS
import com.dimajix.spark.testing.LocalSparkSession


class FileSystemTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "FileSystem.local" should "be usable with simple strings" in {
        val prefix = if (FileSystem.WINDOWS) "file:/" else "file:"
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val dir1 = fs.local(tempDir.toString)
        fs.local(dir1.toString) should be (dir1)
        fs.local(dir1.path) should be(dir1)
        fs.local(dir1.uri) should be(dir1)
        dir1.uri should be (tempDir.toURI)
        dir1.path should be (new Path(tempDir.toURI))
        dir1.toString should be (prefix + tempDir.toString.replace('\\', '/'))
        dir1.toString.takeRight(1) should not be ("/")
        dir1.exists() should be (true)
        dir1.isFile() should be (false)
        dir1.isDirectory() should be (true)

        val dir2 = fs.local(tempDir.toString + "/lolo")
        fs.local(dir2.toString) should be (dir2)
        fs.local(dir2.path) should be (dir2)
        fs.local(dir2.uri) should be (dir2)
        dir2.uri should be(tempDir.toURI.resolve("lolo"))
        dir2.path should be(new Path(tempDir.toURI.resolve("lolo")))
        dir2.path should be (new Path(tempDir.toURI.resolve("lolo").toString))
        dir2.name should be ("lolo")
        dir2.toString.takeRight(1) should not be ("/")
        dir2.exists() should be(false)
        dir2.isFile() should be(false)
        dir2.isDirectory() should be(false)

        val dir3 = fs.local(tempDir.toString + "/lolo/")
        fs.local(dir3.toString) should be(dir3)
        fs.local(dir3.path) should be(dir3)
        fs.local(dir3.uri) should be(dir3)
        dir3.uri should be(tempDir.toURI.resolve("lolo"))
        dir3.path should be(new Path(tempDir.toURI.resolve("lolo")))
        dir3.path should be(new Path(tempDir.toURI.resolve("lolo").toString))
        dir3.toString.takeRight(1) should not be ("/")
        dir3.name should be("lolo")
        dir3.exists() should be(false)
        dir3.isFile() should be(false)
        dir3.isDirectory() should be(false)
    }

    it should "be usable with URIs" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val dir = fs.local(tempDir.toURI)
        dir.uri should be(tempDir.toURI)
        dir.path should be(new Path(tempDir.toURI))
        dir.toString should be ("file:" + tempDir.toString)
        dir.toString.takeRight(1) should not be ("/")
        //dir.path should be(new Path(tempDir.toURI.toString))
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isDirectory() should be(true)

        val dir2 = fs.local(tempDir.toURI.resolve("lolo"))
        dir2.uri should be(tempDir.toURI.resolve("lolo"))
        dir2.path should be(new Path(tempDir.toURI.resolve("lolo")))
        dir2.path should be(new Path(tempDir.toURI.resolve("lolo").toString))
        dir2.toString.takeRight(1) should not be ("/")
        dir2.name should be("lolo")
        dir2.exists() should be(false)
        dir2.isFile() should be(false)
        dir2.isDirectory() should be(false)

        val dir3 = fs.local(tempDir.toURI.resolve("lolo/"))
        dir3.uri should be(tempDir.toURI.resolve("lolo"))
        dir3.path should be(new Path(tempDir.toURI.resolve("lolo")))
        dir3.path should be(new Path(tempDir.toURI.resolve("lolo").toString))
        dir3.toString.takeRight(1) should not be ("/")
        dir3.name should be("lolo")
        dir3.exists() should be(false)
        dir3.isFile() should be(false)
        dir3.isDirectory() should be(false)
    }

    it should "be usable with Paths" in {
        val prefix = if (FileSystem.WINDOWS) "file:/" else "file:"
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val dir = fs.local(new Path(tempDir.toString))
        dir.path should be(new Path(tempDir.toURI))
        dir.uri should be(tempDir.toURI)
        dir.toString should be (prefix + tempDir.toString.replace('\\', '/'))
        dir.toString.takeRight(1) should not be ("/")
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isDirectory() should be(true)

        val dir1 = dir / "lala"
        dir1.uri should be(new Path(new Path(tempDir.toURI), "lala").toUri)
        dir1.path should be(new Path(new Path(tempDir.toURI), "lala"))
        dir1.name should be("lala")
        val file = dir1 / "lolo.tmp"
        file.uri should be(new Path(dir1.path, "lolo.tmp").toUri)
        file.path should be(new Path(dir1.path, "lolo.tmp"))
        file.name should be("lolo.tmp")

        val dir2 = fs.local(new Path(tempDir.toURI.resolve("lolo")))
        dir2.uri should be(tempDir.toURI.resolve("lolo"))
        dir2.path should be(new Path(tempDir.toURI.resolve("lolo")))
        dir2.path should be(new Path(tempDir.toURI.resolve("lolo").toString))
        dir2.toString.takeRight(1) should not be ("/")
        dir2.name should be("lolo")
        dir2.exists() should be(false)
        dir2.isFile() should be(false)
        dir2.isDirectory() should be(false)

        val dir3 = fs.local(new Path(tempDir.toURI.resolve("lolo/")))
        dir3.uri should be(tempDir.toURI.resolve("lolo"))
        dir3.path should be(new Path(tempDir.toURI.resolve("lolo")))
        dir3.path should be(new Path(tempDir.toURI.resolve("lolo").toString))
        dir3.toString.takeRight(1) should not be ("/")
        dir3.name should be("lolo")
        dir3.exists() should be(false)
        dir3.isFile() should be(false)
        dir3.isDirectory() should be(false)
    }

    it should "be usable with Files" in {
        val prefix = if (FileSystem.WINDOWS) "file:/" else "file:"
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val dir = fs.local(tempDir)
        dir.uri should be (tempDir.toURI)
        dir.path should be(new Path(tempDir.toURI))
        dir.toString should be (prefix + tempDir.toString.replace('\\', '/'))
        //dir.path should be(new Path(tempDir.toURI.toString))
        dir.exists() should be (true)
        dir.isFile() should be (false)
        dir.isDirectory() should be (true)
    }

    if (!WINDOWS) it should "be usable with special characters and whitespaces (String)" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.local("/tmp/hourly/hour=2022-03-10 20:00:00")
        fs.local(file.toString) should be(file)
        fs.local(file.path) should be(file)
        fs.local(file.uri) should be(file)
        file.uri should be (new URI("file:/tmp/hourly/hour=2022-03-10%2020:00:00"))
        file.path should be (new Path("file:/tmp/hourly/hour=2022-03-10 20:00:00"))
        file.toString should be ("file:/tmp/hourly/hour=2022-03-10 20:00:00")

        val file2 = fs.local("/tmp/hourly/hour=2022-03-10%20:00:00")
        file2.uri should be(new URI("file:/tmp/hourly/hour=2022-03-10%2520:00:00"))
        file2.path should be (new Path("file:/tmp/hourly/hour=2022-03-10%20:00:00"))
        file2.toString should be("file:/tmp/hourly/hour=2022-03-10%20:00:00")
    }

    if (!WINDOWS) it should "be usable with special characters and whitespaces (Path)" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.local(new Path("/tmp/hourly/hour=2022-03-10 20:00:00"))
        fs.local(file.toString) should be(file)
        fs.local(file.path) should be(file)
        fs.local(file.uri) should be(file)
        file.uri should be(new URI("file:/tmp/hourly/hour=2022-03-10%2020:00:00"))
        file.path should be (new Path("file:/tmp/hourly/hour=2022-03-10 20:00:00"))
        file.toString should be("file:/tmp/hourly/hour=2022-03-10 20:00:00")

        val file2 = fs.local(new Path("/tmp/hourly/hour=2022-03-10%20:00:00"))
        file2.uri should be(new URI("file:/tmp/hourly/hour=2022-03-10%2520:00:00"))
        file2.path should be (new Path("file:/tmp/hourly/hour=2022-03-10%20:00:00"))
        file2.toString should be("file:/tmp/hourly/hour=2022-03-10%20:00:00")
    }

    if (!WINDOWS) it should "be usable with special characters and whitespaces (File)" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.local(new java.io.File("/tmp/hourly/hour=2022-03-10 20:00:00"))
        fs.local(file.toString) should be(file)
        fs.local(file.path) should be(file)
        fs.local(file.uri) should be(file)
        file.uri should be(new URI("file:/tmp/hourly/hour=2022-03-10%2020:00:00"))
        file.path should be (new Path("file:/tmp/hourly/hour=2022-03-10 20:00:00"))
        file.toString should be("file:/tmp/hourly/hour=2022-03-10 20:00:00")

        val file2 = fs.local(new java.io.File("/tmp/hourly/hour=2022-03-10%20:00:00"))
        file2.uri should be(new URI("file:/tmp/hourly/hour=2022-03-10%2520:00:00"))
        file2.path should be (new Path("file:/tmp/hourly/hour=2022-03-10%20:00:00"))
        file2.toString should be("file:/tmp/hourly/hour=2022-03-10%20:00:00")
    }

    it should "be usable relative paths (String)" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.local("target/classes")
        file.name should be ("classes")
        file.path.isAbsolute should be (true)
        file.uri.isAbsolute should be (true)
        file.toString should be ("file:target/classes")
        file.exists() should be(true)
        file.isFile() should be(false)
        file.isDirectory() should be(true)

        val abs = file.absolute
        abs.name should be("classes")
        abs.path.isAbsolute should be(true)
        abs.uri.isAbsolute should be(true)
        abs.exists() should be(true)
        abs.isFile() should be(false)
        abs.isDirectory() should be(true)
    }

    it should "be usable relative paths (Path)" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.local(new Path("target/classes"))
        file.name should be("classes")
        file.path.isAbsolute should be(true)
        file.uri.isAbsolute should be(true)
        file.toString should be ("file:target/classes")
        file.exists() should be(true)
        file.isFile() should be(false)
        file.isDirectory() should be(true)

        val abs = file.absolute
        abs.name should be("classes")
        abs.path.isAbsolute should be(true)
        abs.uri.isAbsolute should be(true)
        abs.exists() should be(true)
        abs.isFile() should be(false)
        abs.isDirectory() should be(true)
    }

    it should "resolve relative Paths in local(String)" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.local(tempDir.toString + "/lala/../lolo")
        //tmpFromUri.path should be (new Path("file:" + tempDir.toString + "/"))
        file.path should be(new Path(tempDir.toURI.toString + "/lolo"))
        file.uri should be(tempDir.toURI.resolve("lolo"))
        file.exists() should be(false)
        file.isFile() should be(false)
        file.isDirectory() should be(false)

        file.create(true).close
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isDirectory() should be(false)

        file.parent.exists() should be(true)
        file.parent.isFile() should be(false)
        file.parent.isDirectory() should be(true)

        file.delete()
        file.exists() should be(false)
        file.isFile() should be(false)
        file.isDirectory() should be(false)
    }

    it should "resolve relative Paths in local(URI)" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.local(new URI(tempDir.toURI.toString + "/lala2/../lolo2"))
        //tmpFromUri.path should be (new Path("file:" + tempDir.toString + "/"))
        file.path should be(new Path(tempDir.toURI.toString + "/lolo2"))
        file.uri should be(tempDir.toURI.resolve("lolo2"))
        file.exists() should be(false)
        file.isFile() should be(false)
        file.isDirectory() should be(false)

        file.create(true).close
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isDirectory() should be(false)

        file.parent.exists() should be(true)
        file.parent.isFile() should be(false)
        file.parent.isDirectory() should be(true)

        file.delete()
        file.exists() should be(false)
        file.isFile() should be(false)
        file.isDirectory() should be(false)
    }

    it should "support creating entries" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val tmp = fs.local(tempDir)
        val name = "lala-" + System.currentTimeMillis().toString + ".tmp"
        val file = tmp / name
        file.name should be (name)
        file.exists() should be (false)
        file.isFile() should be (false)
        file.isDirectory() should be (false)

        file.create().close()
        file.exists() should be (true)
        file.isFile() should be (true)
        file.isDirectory() should be (false)

        file.delete(false)
        file.exists() should be (false)
        file.isFile() should be (false)
        file.isDirectory() should be (false)
    }

    it should "support renaming entries" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val tmp = fs.local(tempDir)
        val file = tmp / ("lala-" + System.currentTimeMillis().toString + ".tmp")
        file.exists() should be (false)
        file.isFile() should be (false)
        file.isDirectory() should be (false)

        file.create().close()
        file.exists() should be (true)
        file.isFile() should be (true)
        file.isDirectory() should be (false)

        val newName = file.withName("lolo-" + System.currentTimeMillis().toString + ".tmp")
        file.rename(newName.path)
        file.exists() should be (false)
        file.isFile() should be (false)
        file.isDirectory() should be (false)
        newName.exists() should be (true)
        newName.isFile() should be (true)
        newName.isDirectory() should be (false)

        newName.delete(false)
        newName.exists() should be (false)
        newName.isFile() should be (false)
        newName.isDirectory() should be (false)
    }

    "FileSystem.file" should "be usable with simple strings" in {
        val prefix = if (FileSystem.WINDOWS) "file:/" else "file:"
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val dir = fs.file(tempDir.toString)
        fs.file(dir.toString) should be(dir)
        fs.file(dir.path) should be(dir)
        fs.file(dir.uri) should be(dir)
        dir.uri should be(new Path(tempDir.toURI.toString).toUri)
        dir.path should be(new Path(tempDir.toURI.toString))
        dir.toString should be(prefix + tempDir.toString.replace('\\', '/'))
        dir.uri.toString + "/" should be(tempDir.toURI.toString)
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isDirectory() should be(true)

        val dir1 = dir / "lala"
        dir1.uri should be(new Path(new Path(tempDir.toURI), "lala").toUri)
        dir1.path should be(new Path(new Path(tempDir.toURI), "lala"))
        dir1.toString should be(prefix + tempDir.toString.replace('\\', '/') + "/lala")
        dir1.name should be("lala")
        val file = dir1 / "lolo.tmp"
        file.uri should be(new Path(dir1.path, "lolo.tmp").toUri)
        file.path should be(new Path(dir1.path, "lolo.tmp"))
        file.name should be("lolo.tmp")

        val dir2 = fs.local(new Path(tempDir.toURI.resolve("lolo")))
        dir2.uri should be(tempDir.toURI.resolve("lolo"))
        dir2.path should be(new Path(tempDir.toURI.resolve("lolo")))
        dir2.path should be(new Path(tempDir.toURI.resolve("lolo").toString))
        dir2.toString should be(prefix + tempDir.toString.replace('\\', '/') + "/lolo")
        dir2.name should be("lolo")
        dir2.exists() should be(false)
        dir2.isFile() should be(false)
        dir2.isDirectory() should be(false)

        val dir3 = fs.local(new Path(tempDir.toURI.resolve("lolo/")))
        dir3.uri should be(tempDir.toURI.resolve("lolo"))
        dir3.path should be(new Path(tempDir.toURI.resolve("lolo")))
        dir3.path should be(new Path(tempDir.toURI.resolve("lolo").toString))
        dir3.name should be("lolo")
        dir3.exists() should be(false)
        dir3.isFile() should be(false)
        dir3.isDirectory() should be(false)
    }

    it should "be usable with URIs" in {
        val prefix = if (FileSystem.WINDOWS) "file:/" else "file:"
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val dir = fs.file(tempDir.toURI)
        //fs.file(dir.toString) should be(dir)
        fs.file(dir.path) should be(dir)
        fs.file(dir.uri) should be(dir)
        //dir.uri should be(tempDir.toURI)
        dir.uri.toString + "/" should be(tempDir.toURI.toString)
        dir.path should be(new Path(prefix + tempDir.toString))
        dir.toString should be(prefix + tempDir.toString.replace('\\', '/'))
        dir.toString.takeRight(1) should not be ("/")
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isDirectory() should be(true)

        val dir1 = dir / "lala"
        dir1.uri should be(new Path(new Path(tempDir.toURI), "lala").toUri)
        dir1.path should be(new Path(new Path(tempDir.toURI), "lala"))
        dir1.toString.takeRight(1) should not be ("/")
        dir1.name should be("lala")
        val file = dir1 / "lolo.tmp"
        file.uri should be(new Path(dir1.path, "lolo.tmp").toUri)
        file.path should be(new Path(dir1.path, "lolo.tmp"))
        file.name should be("lolo.tmp")

        val dir2 = fs.local(new Path(tempDir.toURI.resolve("lolo")))
        dir2.uri should be(tempDir.toURI.resolve("lolo"))
        dir2.path should be(new Path(tempDir.toURI.resolve("lolo")))
        dir2.path should be(new Path(tempDir.toURI.resolve("lolo").toString))
        dir2.toString.takeRight(1) should not be ("/")
        dir2.name should be("lolo")
        dir2.exists() should be(false)
        dir2.isFile() should be(false)
        dir2.isDirectory() should be(false)

        val dir3 = fs.local(new Path(tempDir.toURI.resolve("lolo/")))
        dir3.uri should be(tempDir.toURI.resolve("lolo"))
        dir3.path should be(new Path(tempDir.toURI.resolve("lolo")))
        dir3.path should be(new Path(tempDir.toURI.resolve("lolo").toString))
        dir3.toString.takeRight(1) should not be ("/")
        dir3.name should be("lolo")
        dir3.exists() should be(false)
        dir3.isFile() should be(false)
        dir3.isDirectory() should be(false)
    }

    it should "be usable with special characters and whitespaces (String)" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.file("/tmp/hourly/hour=2022-03-10 20:00:00")
        fs.file(file.toString) should be(file)
        fs.file(file.path) should be(file)
        fs.file(file.uri) should be(file)
        file.uri should be(new URI("file:/tmp/hourly/hour=2022-03-10%2020:00:00"))
        file.toString should be("file:/tmp/hourly/hour=2022-03-10 20:00:00")
    }

    it should "be usable with special characters and whitespaces (Path)" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.file(new Path("/tmp/hourly/hour=2022-03-10 20:00:00"))
        fs.file(file.toString) should be(file)
        fs.file(file.path) should be(file)
        fs.file(file.uri) should be(file)
        file.uri should be(new URI("file:/tmp/hourly/hour=2022-03-10%2020:00:00"))
        file.toString should be("file:/tmp/hourly/hour=2022-03-10 20:00:00")
    }

    it should "be usable relative paths (String)" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.file("target/classes")
        file.name should be("classes")
        file.path.isAbsolute should be(false)
        file.uri.isAbsolute should be(false)
        file.exists() should be(true)
        file.isFile() should be(false)
        file.isDirectory() should be(true)

        val abs = file.absolute
        abs.name should be("classes")
        abs.path.isAbsolute should be(true)
        abs.uri.isAbsolute should be(true)
        abs.exists() should be(true)
        abs.isFile() should be(false)
        abs.isDirectory() should be(true)
    }

    it should "be usable relative paths (Path)" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.file(new Path("target/classes"))
        file.name should be("classes")
        file.path.isAbsolute should be(false)
        file.uri.isAbsolute should be(false)
        file.exists() should be(true)
        file.isFile() should be(false)
        file.isDirectory() should be(true)

        val abs = file.absolute
        abs.name should be("classes")
        abs.path.isAbsolute should be(true)
        abs.uri.isAbsolute should be(true)
        abs.exists() should be(true)
        abs.isFile() should be(false)
        abs.isDirectory() should be(true)
    }

    it should "resolve relative Paths in file(String)" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.file(tempDir.toString + "/lala/../lolo")
        file.path should be(new Path(tempDir.toURI.toString + "/lolo"))
        file.uri should be(tempDir.toURI.resolve("lolo"))
        file.exists() should be(false)
        file.isFile() should be(false)
        file.isDirectory() should be(false)

        file.create(true).close()
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isDirectory() should be(false)

        file.parent.exists() should be(true)
        file.parent.isFile() should be(false)
        file.parent.isDirectory() should be(true)

        file.delete()
        file.exists() should be(false)
        file.isFile() should be(false)
        file.isDirectory() should be(false)
    }

    it should "resolve relative Paths in file(URI)" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.file(new URI(tempDir.toURI.toString + "/lala/../lolo"))
        file.path should be(new Path(tempDir.toURI.toString + "/lolo"))
        file.uri should be(tempDir.toURI.resolve("lolo"))
        file.parent should be (fs.file(tempDir.toURI))
        file.exists() should be(false)
        file.isFile() should be(false)
        file.isDirectory() should be(false)

        file.create(true).close
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isDirectory() should be(false)

        file.parent.exists() should be(true)
        file.parent.isFile() should be(false)
        file.parent.isDirectory() should be(true)

        file.delete()
        file.exists() should be(false)
        file.isFile() should be(false)
        file.isDirectory() should be(false)
    }

    it should "support creating entries" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val tmp = fs.file(tempDir.toURI)
        val file = tmp / ("lala-" + System.currentTimeMillis().toString + ".tmp")
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

    it should "support renaming entries" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val tmp = fs.file(tempDir.toURI)
        val file = tmp / ("lala-" + System.currentTimeMillis().toString + ".tmp")
        file.parent should be (tmp)
        file.exists() should be(false)
        file.isFile() should be(false)
        file.isDirectory() should be(false)

        file.create().close()
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isDirectory() should be(false)

        val newName = file.withName("lolo-" + System.currentTimeMillis().toString + ".tmp")
        file.rename(newName.path)
        file.exists() should be(false)
        file.isFile() should be(false)
        file.isDirectory() should be(false)
        newName.exists() should be(true)
        newName.isFile() should be(true)
        newName.isDirectory() should be(false)

        newName.delete(false)
        newName.exists() should be(false)
        newName.isFile() should be(false)
        newName.isDirectory() should be(false)
    }

    "FileSystem.resource" should "support resources somewhere" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.resource("com/dimajix/flowman/some-test-resource.txt")
        fs.resource(file.uri) should be (file)
        file.parent should be (fs.resource("com/dimajix/flowman"))
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isAbsolute() should be(true)
        file.isDirectory() should be(false)

        val dir = fs.resource("com/dimajix/flowman")
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isAbsolute() should be(true)
        dir.isDirectory() should be(true)

        a[NoSuchFileException] should be thrownBy (fs.resource("com/dimajix/flowman/no-such-file"))
    }

    it should "support resources in JARs" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.resource("org/apache/spark/SparkContext.class")
        fs.resource(file.uri) should be (file)
        file.parent should be (fs.resource("org/apache/spark"))
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isAbsolute() should be(true)
        file.isDirectory() should be(false)

        val dir = fs.resource("org/apache/spark")
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isAbsolute() should be(true)
        dir.isDirectory() should be(true)

        a[NoSuchFileException] should be thrownBy (fs.resource("org/apache/spark/no-such-file"))
    }

    "FileSystem.file" should "support resources somewhere via 'file(URI)'" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val url = Resources.getURL("com/dimajix/flowman/some-test-resource.txt")
        val file = fs.file(url.toURI)
        file.uri should be (url.toURI)
        file.path should be (new Path(url.toURI))
        file.toString should be (url.toURI.toString)
        file.name should be ("some-test-resource.txt")
        file.parent should be (fs.file(Resources.getURL("com/dimajix/flowman").toURI))
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isAbsolute() should be(true)
        file.isDirectory() should be(false)

        val url2 = Resources.getURL("com/dimajix/flowman/../flowman/some-test-resource.txt")
        val file2 = fs.file(url2.toURI)
        file2.uri should be(url2.toURI)
        file2.path should be(new Path(url2.toURI))
        file2.toString should be (url2.toURI.toString)
        file2.parent should be (fs.file(Resources.getURL("com/dimajix/flowman").toURI))
        file2.name should be("some-test-resource.txt")
        file2.exists() should be(true)
        file2.isFile() should be(true)
        file2.isAbsolute() should be(true)
        file2.isDirectory() should be(false)

        val dir = fs.file(Resources.getURL("com/dimajix/flowman").toURI)
        dir.uri should be(Resources.getURL("com/dimajix/flowman").toURI)
        dir.path should be(new Path(Resources.getURL("com/dimajix/flowman").toURI))
        dir.toString should be(Resources.getURL("com/dimajix/flowman").toString)
        dir.name should be("flowman")
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isAbsolute() should be(true)
        dir.isDirectory() should be(true)

        val dir2 = fs.file(Resources.getURL("com/dimajix/flowman/").toURI)
        dir2.uri should be(Resources.getURL("com/dimajix/flowman").toURI)
        dir2.path should be(new Path(Resources.getURL("com/dimajix/flowman").toURI))
        dir2.name should be("flowman")
        dir2.exists() should be(true)
        dir2.isFile() should be(false)
        dir2.isAbsolute() should be(true)
        dir2.isDirectory() should be(true)
    }

    it should "support resources somewhere via 'file(Path)'" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.file(new Path(Resources.getURL("com/dimajix/flowman/some-test-resource.txt").toString))
        file.uri should be(Resources.getURL("com/dimajix/flowman/some-test-resource.txt").toURI)
        file.path should be(new Path(Resources.getURL("com/dimajix/flowman/some-test-resource.txt").toURI))
        file.parent should be (fs.file(Resources.getURL("com/dimajix/flowman").toURI))
        file.name should be("some-test-resource.txt")
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isAbsolute() should be(true)
        file.isDirectory() should be(false)

        val file2 = fs.file(new Path(Resources.getURL("com/dimajix/flowman/../flowman/some-test-resource.txt").toString))
        file2.uri should be(Resources.getURL("com/dimajix/flowman/some-test-resource.txt").toURI)
        file2.path should be(new Path(Resources.getURL("com/dimajix/flowman/some-test-resource.txt").toURI))
        file2.parent should be (fs.file(Resources.getURL("com/dimajix/flowman").toURI))
        file2.name should be("some-test-resource.txt")
        file2.exists() should be(true)
        file2.isFile() should be(true)
        file2.isAbsolute() should be(true)
        file2.isDirectory() should be(false)

        val dir = fs.file(new Path(Resources.getURL("com/dimajix/flowman").toString))
        dir.uri should be(Resources.getURL("com/dimajix/flowman").toURI)
        dir.path should be(new Path(Resources.getURL("com/dimajix/flowman").toURI))
        dir.name should be("flowman")
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isAbsolute() should be(true)
        dir.isDirectory() should be(true)

        val dir2 = fs.file(new Path(Resources.getURL("com/dimajix/flowman/").toString))
        dir2.uri should be(Resources.getURL("com/dimajix/flowman").toURI)
        dir2.path should be(new Path(Resources.getURL("com/dimajix/flowman").toURI))
        dir2.name should be("flowman")
        dir2.exists() should be(true)
        dir2.isFile() should be(false)
        dir2.isAbsolute() should be(true)
        dir2.isDirectory() should be(true)
    }

    it should "support resources somewhere via 'file(String)'" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.file(Resources.getURL("com/dimajix/flowman/some-test-resource.txt").toString)
        file.uri should be(Resources.getURL("com/dimajix/flowman/some-test-resource.txt").toURI)
        file.path should be(new Path(Resources.getURL("com/dimajix/flowman/some-test-resource.txt").toURI))
        file.name should be("some-test-resource.txt")
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isAbsolute() should be(true)
        file.isDirectory() should be(false)

        val file2 = fs.file(Resources.getURL("com/dimajix/flowman/../flowman/some-test-resource.txt").toString)
        file2.uri should be(Resources.getURL("com/dimajix/flowman/some-test-resource.txt").toURI)
        file2.path should be(new Path(Resources.getURL("com/dimajix/flowman/some-test-resource.txt").toURI))
        file2.name should be("some-test-resource.txt")
        file2.exists() should be(true)
        file2.isFile() should be(true)
        file2.isAbsolute() should be(true)
        file2.isDirectory() should be(false)

        val dir = fs.file(Resources.getURL("com/dimajix/flowman").toString)
        dir.uri should be(Resources.getURL("com/dimajix/flowman").toURI)
        dir.path should be(new Path(Resources.getURL("com/dimajix/flowman").toURI))
        dir.name should be("flowman")
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isAbsolute() should be(true)
        dir.isDirectory() should be(true)

        val dir2 = fs.file(Resources.getURL("com/dimajix/flowman/").toString)
        dir2.uri should be(Resources.getURL("com/dimajix/flowman").toURI)
        dir2.path should be(new Path(Resources.getURL("com/dimajix/flowman").toURI))
        dir2.name should be("flowman")
        dir2.exists() should be(true)
        dir2.isFile() should be(false)
        dir2.isAbsolute() should be(true)
        dir2.isDirectory() should be(true)
    }

    it should "support resources in JARs via 'file(URI)'" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.file(Resources.getURL("org/apache/spark/SparkContext.class").toURI)
        file.uri should be(Resources.getURL("org/apache/spark/SparkContext.class").toURI)
        file.path should be(new Path(Resources.getURL("org/apache/spark/SparkContext.class").toURI))
        file.name should be("SparkContext.class")
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isAbsolute() should be(true)
        file.isDirectory() should be(false)

        val dir = fs.file(Resources.getURL("org/apache/spark").toURI)
        dir.uri should be(Resources.getURL("org/apache/spark").toURI)
        dir.path should be(new Path(Resources.getURL("org/apache/spark").toURI))
        dir.name should be("spark")
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isAbsolute() should be(true)
        dir.isDirectory() should be(true)

        // TODO
        val dir2 = fs.file(Resources.getURL("org/apache/spark/").toURI)
        dir2.uri should be(Resources.getURL("org/apache/spark").toURI)
        dir2.path should be(new Path(Resources.getURL("org/apache/spark").toURI))
        dir2.name should be("spark")
        dir2.exists() should be(true)
        dir2.isFile() should be(false)
        dir2.isAbsolute() should be(true)
        dir2.isDirectory() should be(true)
    }

    it should "support resources in JARs via 'file(Path)'" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.file(new Path(Resources.getURL("org/apache/spark/SparkContext.class").toURI))
        file.uri should be(Resources.getURL("org/apache/spark/SparkContext.class").toURI)
        file.path should be(new Path(Resources.getURL("org/apache/spark/SparkContext.class").toURI))
        file.name should be("SparkContext.class")
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isAbsolute() should be(true)
        file.isDirectory() should be(false)

        val dir = fs.file(new Path(Resources.getURL("org/apache/spark").toURI))
        dir.uri should be(Resources.getURL("org/apache/spark").toURI)
        dir.path should be(new Path(Resources.getURL("org/apache/spark").toURI))
        dir.name should be("spark")
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isAbsolute() should be(true)
        dir.isDirectory() should be(true)

        // TODO
        val dir2 = fs.file(new Path(Resources.getURL("org/apache/spark/").toURI))
        dir2.uri should be(Resources.getURL("org/apache/spark").toURI)
        dir2.path should be(new Path(Resources.getURL("org/apache/spark").toURI))
        dir2.name should be("spark")
        dir2.exists() should be(true)
        dir2.isFile() should be(false)
        dir2.isAbsolute() should be(true)
        dir2.isDirectory() should be(true)
    }

    it should "support resources in JARs via 'file(String)'" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.file(Resources.getURL("org/apache/spark/SparkContext.class").toString)
        file.uri should be(Resources.getURL("org/apache/spark/SparkContext.class").toURI)
        file.path should be(new Path(Resources.getURL("org/apache/spark/SparkContext.class").toURI))
        file.name should be("SparkContext.class")
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isAbsolute() should be(true)
        file.isDirectory() should be(false)

        val dir = fs.file(Resources.getURL("org/apache/spark").toString)
        dir.uri should be(Resources.getURL("org/apache/spark").toURI)
        dir.path should be(new Path(Resources.getURL("org/apache/spark").toURI))
        dir.name should be("spark")
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isAbsolute() should be(true)
        dir.isDirectory() should be(true)

        // TODO
        val dir2 = fs.file(Resources.getURL("org/apache/spark/").toString)
        dir2.uri should be(Resources.getURL("org/apache/spark").toURI)
        dir2.path should be(new Path(Resources.getURL("org/apache/spark").toURI))
        dir2.name should be("spark")
        dir2.exists() should be(true)
        dir2.isFile() should be(false)
        dir2.isAbsolute() should be(true)
        dir2.isDirectory() should be(true)
    }
}
