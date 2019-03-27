/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.VarcharType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier


object LatestMapping {
    def apply(input:String, keyColumns:Seq[String], versionColumn:String) : LatestMapping = {
        val mapping = new LatestMapping
        mapping._input = input
        mapping._keyColumns = keyColumns
        mapping._versionColumn = versionColumn
        mapping
    }
}

class LatestMapping extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[LatestMapping])

    @JsonProperty(value = "input", required = true) private var _input:String = _
    @JsonProperty(value = "versionColumn", required = true) private var _versionColumn:String = _
    @JsonProperty(value = "keyColumns", required = true) private var _keyColumns:Seq[String] = Seq()

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def versionColumn(implicit context: Context) : String = context.evaluate(_versionColumn)
    def keyColumns(implicit context: Context) : Seq[String] = _keyColumns.map(context.evaluate(_))

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param tables
      * @return
      */
    override def execute(executor:Executor, tables:Map[MappingIdentifier,DataFrame]) : DataFrame = {
        implicit val context = executor.context
        val input = this.input
        val keyColumns = this.keyColumns.map(col)
        val versionColumn = this.versionColumn
        logger.info(s"Selecting latest version in '$input' using key columns ${keyColumns.mkString(",")} and version column $versionColumn")

        val df = tables(input)

        // Get appropriate function for extracting latest version
        val latest = df.schema(versionColumn).dataType match {
            case ShortType => records:Seq[Row] => records.maxBy(row => row.getShort(0)).getStruct(1)
            case IntegerType => records:Seq[Row] => records.maxBy(row => row.getInt(0)).getStruct(1)
            case LongType => records:Seq[Row] => records.maxBy(row => row.getLong(0)).getStruct(1)
            case FloatType => records:Seq[Row] => records.maxBy(row => row.getFloat(0)).getStruct(1)
            case DoubleType => records:Seq[Row] => records.maxBy(row => row.getDouble(0)).getStruct(1)
            case _:DecimalType => records:Seq[Row] => records.maxBy(row => row.getDecimal(0)).getStruct(1)
            case TimestampType => records:Seq[Row] => records.maxBy(row => row.getTimestamp(0).getTime).getStruct(1)
            case DateType => records:Seq[Row] => records.maxBy(row => row.getDate(0).toInstant.getEpochSecond).getStruct(1)
            case ByteType => records:Seq[Row] => records.maxBy(row => row.getByte(0)).getStruct(1)
            case StringType => records:Seq[Row] => records.maxBy(row => row.getString(0)).getStruct(1)
            case _:VarcharType => records:Seq[Row] => records.maxBy(row => row.getString(0)).getStruct(1)
        }
        val latest_udf = udf(latest, df.schema)

        // Create projection expression
        val cols = keyColumns :+
            struct(
                // Extract version
                col(versionColumn) as "version",
                // Extract full record
                struct(col("*")) as "record"
            ).as("payload")

        df.select(cols:_*)
            .groupBy(keyColumns:_*)
            .agg(
                collect_list(col("payload")) as "records"
            )
            .select(latest_udf(col("records")) as "record")
            .select(col("record.*"))
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context) : Array[MappingIdentifier] = {
        Array(input)
    }
}
