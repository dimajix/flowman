/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.spark.sql

import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

import com.dimajix.spark.sql.catalyst.PlanUtils
import com.dimajix.spark.sql.local.csv.CsvOptions
import com.dimajix.spark.sql.local.csv.UnivocityReader


object DataFrameUtils {
    private val csvOptions = new CsvOptions(Map())

    def singleRow(sparkSession: SparkSession, schema: StructType): DataFrame = {
        val logicalPlan = PlanUtils.singleRowPlan(schema)
        new Dataset[Row](sparkSession, logicalPlan, RowEncoder(schema))
    }

    def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
        val qe = sparkSession.sessionState.executePlan(logicalPlan)
        qe.assertAnalyzed()
        new Dataset[Row](sparkSession, logicalPlan, RowEncoder(qe.analyzed.schema))
    }

    def ofRows(sparkSession: SparkSession, lines:Seq[Array[String]], schema:StructType) : DataFrame = {
        val reader = new UnivocityReader(schema, csvOptions)
        val rows = lines.map(reader.convert)
        sparkSession.createDataFrame(rows.asJava, schema)
    }

    /**
     * Create a [[DataFrame]] from a sequence of Strings containing CSV data
     * @param sparkSession
     * @param lines
     * @param schema
     * @return
     */
    def ofCsvRows(sparkSession: SparkSession, lines:Seq[String], schema:StructType) : DataFrame = {
        val reader = new UnivocityReader(schema, csvOptions)
        val rows = UnivocityReader.parseIterator(lines.iterator, false, reader).toSeq
        sparkSession.createDataFrame(rows.asJava, schema)
    }

    /**
     * Create an empty [[DataFrame]] from a schema
     * @param sparkSession
     * @param schema
     * @return
     */
    def ofSchema(sparkSession: SparkSession, schema:StructType) : DataFrame = {
        val rdd = sparkSession.sparkContext.emptyRDD[Row]
        sparkSession.createDataFrame(rdd, schema)
    }
}
