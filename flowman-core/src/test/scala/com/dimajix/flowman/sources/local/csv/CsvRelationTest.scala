package com.dimajix.flowman.sources.local.csv

import java.nio.file.Files
import java.nio.file.Path

import org.apache.spark.sql.SaveMode
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.sources.local.implicits._


class CsvRelationTest extends FlatSpec with Matchers with BeforeAndAfter with LocalSparkSession {
    var tempDir:Path = _

    before {
        tempDir = Files.createTempDirectory("csv_relation_test")
    }
    after {
        tempDir.toFile.listFiles().foreach(_.delete())
        tempDir.toFile.delete()
    }

    "The csv relation" should "support writing CSV files" in {
        val df = spark.createDataFrame(Seq((1,"lala", 1.2),(2,"lolo", 2.3)))
        df.writeLocal
            .format("csv")
            .option("encoding", "UTF-8")
            .save(tempDir.resolve("lala.csv").toString, SaveMode.Overwrite)
    }
}
