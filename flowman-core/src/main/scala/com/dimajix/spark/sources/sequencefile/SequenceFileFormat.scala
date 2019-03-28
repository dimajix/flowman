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

package com.dimajix.spark.sources.sequencefile

import scala.collection.immutable.Map

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.CodecStreams
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.hadoop.SerializableConfiguration


class SequenceFileFormat extends DataSourceRegister with FileFormat {
    override def shortName = "sequencefile"

    override def toString = "SequenceFile"

    override def inferSchema(
                       sparkSession: SparkSession,
                       options: Map[String, String],
                       files: Seq[FileStatus]): Option[StructType] = {
        val schema = StructType(
            StructField("key", BinaryType, true) ::
            StructField("value", BinaryType, true) ::
            Nil
        )
        Some(schema)
    }

    override protected def buildReader(
                         sparkSession: SparkSession,
                         dataSchema: StructType,
                         partitionSchema: StructType,
                         requiredSchema: StructType,
                         filters: Seq[Filter],
                         parameters: Map[String, String],
                         hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
        val options = new SequenceFileOptions(sparkSession.sparkContext.hadoopConfiguration, parameters, dataSchema)

        (file:PartitionedFile) => {
            val seqFile = new SequenceFile.Reader(options.hadoopConf,
                SequenceFile.Reader.file(new Path(file.filePath)),
                SequenceFile.Reader.length(file.length),
                SequenceFile.Reader.start(file.start)
            )
            Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => seqFile.close()))
            new SequenceFileIterator(seqFile, options)
        }
    }

    override def prepareWrite(
                         sparkSession: SparkSession,
                         job: Job,
                         parameters: Map[String, String],
                         dataSchema: StructType): OutputWriterFactory = {
        val options = new SequenceFileOptions(sparkSession.sparkContext.hadoopConfiguration, parameters, dataSchema)
        new OutputWriterFactory {
            override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
                new SequenceFileOutputWriter(path, context, options)
            }

            override def getFileExtension(context: TaskAttemptContext): String = {
                ".seq" + CodecStreams.getCompressionExtension(context)
            }
        }
    }
}


private[spark] class SequenceFileIterator(seqFile:SequenceFile.Reader, options:SequenceFileOptions) extends Iterator[InternalRow] {
    private val keyConverter = options.keyConverter
    private val valueConverter = options.valueConverter
    private val keyReader = keyConverter.converter
    private val valueReader = valueConverter.converter

    private val key = keyConverter.writableFactory()
    private val value = valueConverter.writableFactory()
    private var validKeyValue = seqFile.next(key, value)

    override def hasNext: Boolean = {
        validKeyValue
    }

    override def next(): InternalRow = {
        val result = if (options.hasKey) {
            InternalRow(keyReader(key), valueReader(value))
        }
        else {
            InternalRow(valueReader(value))
        }
        validKeyValue = seqFile.next(key, value)
        result
    }
}


private[spark] class SequenceFileOutputWriter(
                 path: String,
                 context: TaskAttemptContext,
                 options: SequenceFileOptions) extends OutputWriter with Logging {

    private val keyConverter = options.keyConverter
    private val valueConverter = options.valueConverter
    private val keyExtractor = keyConverter.writableExtractor
    private val valueExtractor = valueConverter.writableExtractor

    private val writer = SequenceFile.createWriter(
        options.hadoopConf,
        SequenceFile.Writer.file(new Path(path)),
        SequenceFile.Writer.keyClass(keyConverter.writable),
        SequenceFile.Writer.valueClass(valueConverter.writable)
    )

    def write(row: InternalRow): Unit = {
        val key = keyExtractor(row)
        val value  = valueExtractor(row)
        writer.append(key, value)
    }

    def close(): Unit = writer.close()

    def flush(): Unit = writer.hflush()
}