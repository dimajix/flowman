/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Adapted for fixed width format 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.sources.spark

import java.io.Writer

import com.univocity.parsers.fixed.FixedWidthWriter
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.CodecStreams
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.TextBasedFileFormat
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType


class FixedWidthFormat extends TextBasedFileFormat with DataSourceRegister{
    override def shortName = "fixedwidth"

    override def toString = "FixedWidth"

    override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = ???

    override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
        FixedWidthUtils.verifySchema(dataSchema)
        val conf = job.getConfiguration
        val fixedWidthOptions = new FixedWidthOptions(options, sparkSession.sessionState.conf.sessionLocalTimeZone)
        fixedWidthOptions.compressionCodec.foreach { codec =>
            CompressionCodecs.setCodecConfiguration(conf, codec)
        }

        new OutputWriterFactory {
            override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
                new FixedWidthOutputWriter(path, dataSchema, context, fixedWidthOptions)
            }

            override def getFileExtension(context: TaskAttemptContext): String = {
                ".dat" + CodecStreams.getCompressionExtension(context)
            }
        }
    }
}


private[spark] class FixedWidthOutputWriter(
               path: String,
               schema: StructType,
               context: TaskAttemptContext,
               options: FixedWidthOptions) extends OutputWriter with Logging {

    private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path))
    private val writerSettings = options.asWriterSettings(schema)
    writerSettings.setHeaders(schema.fieldNames: _*)
    private val gen = new FixedWidthWriter(writer, writerSettings)
    private var printHeader = options.headerFlag

    // A `ValueConverter` is responsible for converting a value of an `InternalRow` to `String`.
    // When the value is null, this converter should not be called.
    private type ValueConverter = (InternalRow, Int) => String

    // `ValueConverter`s for all values in the fields of the schema
    private val valueConverters: Array[ValueConverter] =
        schema.map(_.dataType).map(makeConverter).toArray

    private def makeConverter(dataType: DataType): ValueConverter = dataType match {
        case DateType =>
            (row: InternalRow, ordinal: Int) =>
                options.dateFormat.format(DateTimeUtils.toJavaDate(row.getInt(ordinal)))

        case TimestampType =>
            (row: InternalRow, ordinal: Int) =>
                options.timestampFormat.format(DateTimeUtils.toJavaTimestamp(row.getLong(ordinal)))

        case dt: DataType =>
            (row: InternalRow, ordinal: Int) =>
                row.get(ordinal, dt).toString
    }

    private def convertRow(row: InternalRow): Seq[String] = {
        var i = 0
        val values = new Array[String](row.numFields)
        while (i < row.numFields) {
            if (!row.isNullAt(i)) {
                values(i) = valueConverters(i).apply(row, i)
            } else {
                values(i) = options.nullValue
            }
            i += 1
        }
        values
    }

    /**
      * Writes a single InternalRow using Univocity.
      */
    def write(row: InternalRow): Unit = {
        if (printHeader) {
            gen.writeHeaders()
        }
        gen.writeRow(convertRow(row): _*)
        printHeader = false
    }

    def close(): Unit = gen.close()

    def flush(): Unit = gen.flush()
}
