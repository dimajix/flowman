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

import java.io.File

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.sources.local.implicits._


class CsvRelationTest extends FlatSpec with Matchers with BeforeAndAfter with LocalSparkSession {
    "The csv relation" should "support writing CSV files" in {
        val df = spark.createDataFrame(Seq((1,"lala", 1.2),(2,"lolo", 2.3)))
        df.writeLocal
            .format("csv")
            .option("encoding", "UTF-8")
            .save(new File(tempDir, "lala.csv"), SaveMode.Overwrite)
    }

    it should "support reading CSV files" in {
        val df = spark.readLocal
            .format("csv")
            .schema(StructType(
                StructField("int_field", IntegerType) ::
                StructField("str_field", StringType) ::
                StructField("double_field", DoubleType) ::
                Nil
            ))
            .option("encoding", "UTF-8")
            .load(new File(tempDir, "lala.csv"))
        val result = df
        result.count() should be (2)
        val rows = result.collect()
        rows(0).getInt(0) should be (1)
        rows(0).getString(1) should be ("lala")
        rows(0).getDouble(2) should be (1.2)
        rows(1).getInt(0) should be (2)
        rows(1).getString(1) should be ("lolo")
        rows(1).getDouble(2) should be (2.3)
    }
}
