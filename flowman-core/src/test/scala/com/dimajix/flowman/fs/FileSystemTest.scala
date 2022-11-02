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

import java.nio.file.NoSuchFileException

import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.Resources
import com.dimajix.spark.testing.LocalSparkSession


class FileSystemTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "FileSystem.local" should "be usable with simple strings" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val tmpFromString = fs.local(tempDir.toString)
        tmpFromString.exists() should be (true)
        tmpFromString.isFile() should be (false)
        tmpFromString.isDirectory() should be (true)
    }

    it should "be usable with Files" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val tmpFromUri = fs.local(tempDir)
        tmpFromUri.exists() should be (true)
        tmpFromUri.isFile() should be (false)
        tmpFromUri.isDirectory() should be (true)
    }

    it should "be usable relative paths" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.local("target/classes")
        file.exists() should be(true)
        file.isFile() should be(false)
        file.isDirectory() should be(true)
    }

    it should "be usable with Paths" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val tmpFromUri = fs.local(new Path(tempDir.toString))
        tmpFromUri.exists() should be(true)
        tmpFromUri.isFile() should be(false)
        tmpFromUri.isDirectory() should be(true)
    }

    it should "be usable with URIs" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val tmpFromUri = fs.local(tempDir.toURI)
        tmpFromUri.exists() should be (true)
        tmpFromUri.isFile() should be (false)
        tmpFromUri.isDirectory() should be (true)
    }

    it should "support creating entries" in {
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
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val tmpFromString = fs.file(tempDir.toString)
        tmpFromString.exists() should be(true)
        tmpFromString.isFile() should be(false)
        tmpFromString.isDirectory() should be(true)
    }

    it should "be usable with URIs" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val tmpFromUri = fs.file(tempDir.toURI)
        tmpFromUri.exists() should be(true)
        tmpFromUri.isFile() should be(false)
        tmpFromUri.isDirectory() should be(true)
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
        val file = fs.resource("com/dimajix/flowman/flowman.properties")
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
        val file = fs.file(Resources.getURL("com/dimajix/flowman/flowman.properties").toURI)
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isAbsolute() should be(true)
        file.isDirectory() should be(false)

        val dir = fs.file(Resources.getURL("com/dimajix/flowman").toURI)
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isAbsolute() should be(true)
        dir.isDirectory() should be(true)
    }

    it should "support resources somewhere via 'file(String)'" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.file(Resources.getURL("com/dimajix/flowman/flowman.properties").toString)
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isAbsolute() should be(true)
        file.isDirectory() should be(false)

        val dir = fs.file(Resources.getURL("com/dimajix/flowman").toString)
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isAbsolute() should be(true)
        dir.isDirectory() should be(true)
    }

    it should "support resources in JARs via 'file(URI)'" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.file(Resources.getURL("org/apache/spark/SparkContext.class").toURI)
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isAbsolute() should be(true)
        file.isDirectory() should be(false)

        val dir = fs.file(Resources.getURL("org/apache/spark").toURI)
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isAbsolute() should be(true)
        dir.isDirectory() should be(true)
    }

    it should "support resources in JARs via 'file(String)'" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val file = fs.file(Resources.getURL("org/apache/spark/SparkContext.class").toString)
        file.exists() should be(true)
        file.isFile() should be(true)
        file.isAbsolute() should be(true)
        file.isDirectory() should be(false)

        val dir = fs.file(Resources.getURL("org/apache/spark").toString)
        dir.exists() should be(true)
        dir.isFile() should be(false)
        dir.isAbsolute() should be(true)
        dir.isDirectory() should be(true)
    }
}
