package com.dimajix.flowman.fs

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
