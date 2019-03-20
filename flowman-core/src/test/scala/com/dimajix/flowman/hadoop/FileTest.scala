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

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession


class FileTest extends FlatSpec with Matchers with LocalSparkSession {
    "A local File" should "be useable with simple strings" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val tmpFromString = fs.local(tempDir.toString)
        tmpFromString.exists() should be (true)
        tmpFromString.isFile() should be (false)
        tmpFromString.isDirectory() should be (true)
    }

    it should "be useable with Files" in {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem(conf)
        val tmpFromUri = fs.local(tempDir)
        tmpFromUri.exists() should be (true)
        tmpFromUri.isFile() should be (false)
        tmpFromUri.isDirectory() should be (true)
    }

    it should "be useable with URIs" in {
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
        file.rename(newName)
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
}
