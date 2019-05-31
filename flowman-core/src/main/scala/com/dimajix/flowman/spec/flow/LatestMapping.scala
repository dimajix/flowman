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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.udf
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
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.util.SchemaUtils


case class LatestMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    keyColumns:Seq[String],
    versionColumn:String
) extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[LatestMapping])

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param tables
      * @return
      */
    override def execute(executor:Executor, tables:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(executor != null)
        require(tables != null)

        logger.info(s"Selecting latest version in '$input' using key columns ${keyColumns.mkString(",")} and version column $versionColumn")

        val df = tables(input)

        val result = execute_window(df, keyColumns, versionColumn)

        Map("default" -> result)
    }

    /**
      * Spark native implementation using Window functions
      * @param df
      * @param keyColumns
      * @param versionColumn
      * @return
      */
    private def execute_window(df:DataFrame, keyColumns:Seq[String], versionColumn:String) = {
        val window = Window.partitionBy(keyColumns.map(col):_*).orderBy(col(versionColumn).desc)
        df.select(struct(col("*")) as "record", row_number().over(window) as "rank")
            .filter(col("rank") === 1)
            .select(col("record.*"))
    }

    /**
      * Alternative implementation using UDFs
      * @param df
      * @param keyColumns
      * @param versionColumn
      * @return
      */
    private def execute_udf(df:DataFrame, keyColumns:Seq[String], versionColumn:String) = {
        // Get appropriate function for extracting latest version
        val versionField = SchemaUtils.find(df.schema,versionColumn)
            .getOrElse(throw new IllegalArgumentException(s"Version column $versionColumn not found in schema ${df.schema}"))
        val latest = versionField.dataType match {
            case ShortType => records:Seq[Row] => records.maxBy(row => if (row.isNullAt(0)) Short.MinValue else row.getShort(0)).getStruct(1)
            case IntegerType => records:Seq[Row] => records.maxBy(row => if (row.isNullAt(0)) Int.MinValue else row.getInt(0)).getStruct(1)
            case LongType => records:Seq[Row] => records.maxBy(row => if (row.isNullAt(0)) Long.MinValue else row.getLong(0)).getStruct(1)
            case FloatType => records:Seq[Row] => records.maxBy(row => if (row.isNullAt(0)) Float.MinValue else row.getFloat(0)).getStruct(1)
            case DoubleType => records:Seq[Row] => records.maxBy(row => if (row.isNullAt(0)) Double.MinValue else row.getDouble(0)).getStruct(1)
            case _:DecimalType => records:Seq[Row] => records.maxBy(row => if (row.isNullAt(0)) new java.math.BigDecimal(Long.MinValue) else row.getDecimal(0)).getStruct(1)
            case TimestampType => records:Seq[Row] => records.maxBy(row => if (row.isNullAt(0)) Long.MinValue else row.getTimestamp(0).getTime).getStruct(1)
            case DateType => records:Seq[Row] => records.maxBy(row => if (row.isNullAt(0)) Long.MinValue else row.getDate(0).toInstant.getEpochSecond).getStruct(1)
            case ByteType => records:Seq[Row] => records.maxBy(row => if (row.isNullAt(0)) Byte.MinValue else row.getByte(0)).getStruct(1)
            case StringType => records:Seq[Row] => records.maxBy(row => if (row.isNullAt(0)) "" else row.getString(0)).getStruct(1)
            case _:VarcharType => records:Seq[Row] => records.maxBy(row => if (row.isNullAt(0)) "" else row.getString(0)).getStruct(1)
        }
        val latest_udf = udf(latest, df.schema)

        // Create projection expression
        val cols = Seq(
            struct(keyColumns.map(col):_*) as "key",
            struct(
                // Extract version
                col(versionColumn) as "version",
                // Extract full record
                struct(col("*")) as "record"
            ) as "payload"
        )

        df.select(cols:_*)
            .groupBy(col("key"))
            .agg(
                collect_list(col("payload")) as "records"
            )
            .select(latest_udf(col("records")) as "record")
            .select(col("record.*"))
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @return
      */
    override def dependencies : Seq[MappingOutputIdentifier] = {
        Seq(input)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(input != null)
        val result = input(this.input)

        Map("default" -> result)
    }
}


class LatestMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input:String = _
    @JsonProperty(value = "versionColumn", required = true) private var versionColumn:String = _
    @JsonProperty(value = "keyColumns", required = true) private var keyColumns:Seq[String] = Seq()

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): LatestMapping = {
        LatestMapping(
            instanceProperties(context),
            MappingOutputIdentifier(context.evaluate(input)),
            keyColumns.map(context.evaluate),
            context.evaluate(versionColumn)
        )
    }
}
