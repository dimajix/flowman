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
