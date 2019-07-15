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

package com.dimajix.flowman.transforms

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.spark.testing.LocalSparkSession


class SchemaEnforcerTest extends FlatSpec with Matchers with LocalSparkSession {
    "A conforming schema" should "be generated for simple cases" in {
        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType)
        ))
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))

        val xfs = SchemaEnforcer(requestedSchema)
        val columns = xfs.transform(inputSchema)
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = inputDf.select(columns:_*)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        )))
    }

    it should "support a list of columns an types" in {
        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType)
        ))
        val requestedSchema = Seq(
            "col2" -> "string",
            "col1" -> "string",
            "col4" -> "int"
        )

        val xfs = SchemaEnforcer(requestedSchema)
        val columns = xfs.transform(inputSchema)
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = inputDf.select(columns:_*)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        )))
    }

    it should "work with nested entities" in {
        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", StructType(
                Seq(
                    StructField("nested1", StringType),
                    StructField("nested3", FloatType),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType),
                            StructField("nested4_2", FloatType)
                        )
                    )),
                    StructField("nested5", StructType(
                        Seq(
                            StructField("nested5_1", StringType),
                            StructField("nested5_2", FloatType)
                        )
                    ))
                )
            )),
            StructField("col3", IntegerType)
        ))
        val requestedSchema = StructType(Seq(
            StructField("col2", StructType(
                Seq(
                    StructField("nested1", LongType),
                    StructField("nested2", FloatType),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType),
                            StructField("nested4_3", FloatType)
                        )
                    )),
                    StructField("nested5", StructType(
                        Seq(
                            StructField("nested5_1", StringType),
                            StructField("nested5_2", FloatType)
                        )
                    ))
                )
            )),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))

        val xfs = SchemaEnforcer(requestedSchema)
        val columns = xfs.transform(inputSchema)
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = inputDf.select(columns:_*)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StructType(
                Seq(
                    StructField("nested1", LongType),
                    StructField("nested2", FloatType),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType),
                            StructField("nested4_3", FloatType)
                        )
                    )),
                    StructField("nested5", StructType(
                        Seq(
                            StructField("nested5_1", StringType),
                            StructField("nested5_2", FloatType)
                        )
                    ))
                )
            )),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        )))
    }
}
