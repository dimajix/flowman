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

package com.dimajix.spark.sources.empty

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession


class NullFormatTest extends FlatSpec with Matchers with LocalSparkSession {

    val schema = StructType(
        StructField("s", StringType, metadata = new MetadataBuilder().putLong("size", 8).build()) ::
            StructField("i", IntegerType, metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("b", BooleanType, metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("flt", FloatType, metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("dbl", DoubleType, metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("d", DecimalType(12,2), metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("ts", TimestampType, metadata = new MetadataBuilder().putLong("size", 26).build()) ::
            StructField("dt", DateType, metadata = new MetadataBuilder().putLong("size", 10).build()) ::
            Nil
    )

    "The EmptyFormat" should "support writing with a file name" in {
        val spark = this.spark

        val df = spark.createDataFrame(sc.emptyRDD[Row], schema)
        df.write
            .format("null")
            .save(tempDir + "/null-01")
    }

    it should "support writing with no file name" in {
        val spark = this.spark

        val df = spark.createDataFrame(sc.emptyRDD[Row], schema)
        df.write
            .format("null")
            .save()
    }

    it should "support reading with a file name" in {
        val df = spark.read
            .format("null")
            .schema(schema)
            .load(tempDir + "/null-01")
        df.schema should be (schema)
        df.count() should be (0)
    }
}
