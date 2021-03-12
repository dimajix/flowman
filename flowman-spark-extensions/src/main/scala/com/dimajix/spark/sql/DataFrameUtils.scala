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
import scala.util.Failure
import scala.util.Success

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.BadRecordException
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import com.dimajix.spark.sql.catalyst.PlanUtils
import com.dimajix.spark.sql.local.csv.CsvOptions
import com.dimajix.spark.sql.local.csv.UnivocityReader


object DataFrameUtils {
    private val csvOptions = new CsvOptions(Map())
    private val rowParserOptions = RowParser.Options()

    def singleRow(sparkSession: SparkSession, schema: StructType): DataFrame = {
        val logicalPlan = PlanUtils.singleRowPlan(schema)
        new Dataset[Row](sparkSession, logicalPlan, RowEncoder(schema))
    }

    def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
        val qe = sparkSession.sessionState.executePlan(logicalPlan)
        qe.assertAnalyzed()
        new Dataset[Row](sparkSession, logicalPlan, RowEncoder(qe.analyzed.schema))
    }

    def ofRows(sparkSession: SparkSession, rows:Seq[Row], schema:StructType): DataFrame = {
        sparkSession.createDataFrame(rows.asJava, schema)
    }

    /**
     * Temporarily caches a set of DataFrames
     * @param input
     * @param level
     * @param fn
     * @tparam T
     * @return
     */
    def withCaches[T](input:Iterable[DataFrame], level:StorageLevel=StorageLevel.MEMORY_AND_DISK)(fn: => T) : T = {
        // Cache all DataFrames, and memorize their original storage levels
        val originalPersistedState = input.toSeq.map { df =>
            val originalStorageLevel = df.storageLevel
            if (originalStorageLevel == StorageLevel.NONE)
                df.persist(level)

            df -> originalStorageLevel
        }

        val result = try {
            fn
        }
        finally {
            // Restore previous storage level
            originalPersistedState.foreach { case (df, level) =>
                if (df.storageLevel != level) {
                    if (level == StorageLevel.NONE)
                        df.unpersist()
                    else
                        df.persist(level)
                }
            }
        }

        result
    }

    def withTempViews[T](input:Iterable[(String,DataFrame)])(fn: => T) : T = {
        // Register all input DataFrames as temp views
        input.foreach(kv => kv._2.createOrReplaceTempView(kv._1))

        val result = try {
            fn
        }
        finally {
            // Call SessionCatalog.dropTempView to avoid unpersisting the possibly cached dataset.
            input.foreach(kv => kv._2.sparkSession.sessionState.catalog.dropTempView(kv._1))
        }

        result
    }

    /**
     * Creates a DataFrame from a sequence of string array records
     * @param sparkSession
     * @param lines
     * @param schema
     * @return
     */
    def ofStringValues(sparkSession: SparkSession, lines:Seq[Array[String]], schema:StructType) : DataFrame = {
        val reader = new RowParser(schema, rowParserOptions)
        val rows = lines.map(reader.parse)
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

    def compare(left:DataFrame, right:DataFrame) : Boolean = {
        val leftRows = left.collect().toSeq
        val rightRows = right.collect().toSeq
        compare(leftRows, rightRows)
    }

    /**
     * Compare two DataFrames. They are considered to be equal if their schema and all records match. The order of
     * the records may be different, though.
     * @param left
     * @param right
     * @return
     */
    def compare(left:Seq[Row], right:Seq[Row]) : Boolean = {
        normalizeRows(left) == normalizeRows(right)
    }

    /**
     * Compares two DataFrames and creates a textual diff if they don't contain the same records
     * @param left
     * @param right
     * @return
     */
    def diff(expected:DataFrame, actual:DataFrame) : Option[String] = {
        val expectedRows = expected.collect().toSeq
        val actualRows = actual.collect().toSeq

        if (!compare(expectedRows, actualRows))
            Some(genError(expectedRows, actualRows))
        else
            None
    }
    def diff(expected:Seq[Row], actual:Seq[Row]) : Option[String] = {
        if (!compare(expected, actual))
            Some(genError(expected, actual))
        else
            None
    }

    def diffToStringValues(expected:Seq[Array[String]], actual:DataFrame) : Option[String] = {
        val schema = actual.schema
        val actualRows = actual.collect()

        val expectedRows = try {
            val parser = new RowParser(schema, RowParser.Options())
            Left(expected.map(parser.parse))
        }
        catch {
            case _:BadRecordException =>
                Right(s"Cannot parse expected records with actual schema. Actual schema is:\n${schema.treeString}")
        }

        expectedRows match {
            case Left(expectedRows) =>
                DataFrameUtils.diff(expectedRows, actualRows) match {
                    case Some(diff) =>
                        Some(s"Difference between datasets: \n${diff}")
                    case None =>
                        None
                }
            case Right(error) =>
                Some(error)
        }
    }

    /**
     * Converts data to types that we can do equality comparison using Scala collections.  For BigDecimal type,
     * the Scala type has a better definition of equality test (similar to Java's java.math.BigDecimal.compareTo).
     * For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for equality test.
     * @param rows
     * @return
     */
    def normalizeRows(rows: Seq[Row]): Seq[Row] = {
        def prepareRow(row: Row): Row = {
            Row.fromSeq(row.toSeq.map {
                case null => null
                case d: java.math.BigDecimal => BigDecimal(d)
                // Convert array to Seq for easy equality checkJob.
                case b: Array[_] => b.toSeq
                case r: Row => prepareRow(r)
                case o => o
            })
        }
        rows.map(prepareRow).sortBy(_.toString())
    }

    private def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
        val maxLeftSize = left.map(_.length).max
        val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))("")
        val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))("")

        leftPadded.zip(rightPadded).map {
            case (l, r) => (if (l == r) " " else "!") + l + (" " * ((maxLeftSize - l.length) + 3)) + r
        }
    }

    private def genError(expectedAnswer: Seq[Row],
                         actualAnswer: Seq[Row]): String = {
        val getRowType: Option[Row] => String = row =>
            row.map(row =>
                if (row.schema == null) {
                    "struct<>"
                } else {
                    s"${row.schema.catalogString}"
                }).getOrElse("struct<>")

        s"""
           |== Results ==
           |${
            sideBySide(
                s"== Expected - ${expectedAnswer.size} ==" +:
                    getRowType(expectedAnswer.headOption) +:
                    normalizeRows(expectedAnswer).map(_.toString()),
                s"== Actual - ${actualAnswer.size} ==" +:
                    getRowType(actualAnswer.headOption) +:
                    normalizeRows(actualAnswer).map(_.toString())).mkString("\n")
        }
    """.stripMargin
    }
}
