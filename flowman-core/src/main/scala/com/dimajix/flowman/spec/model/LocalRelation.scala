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

package com.dimajix.flowman.spec.model

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.sources.local.implicits._
import com.dimajix.flowman.spec.schema.FieldValue
import com.dimajix.flowman.spec.schema.PartitionField
import com.dimajix.flowman.spec.schema.SingleValue
import com.dimajix.flowman.util.FileCollector
import com.dimajix.flowman.util.SchemaUtils


class LocalRelation extends BaseRelation {
    private val logger = LoggerFactory.getLogger(classOf[LocalRelation])

    @JsonProperty(value="location") private var _location: String = _
    @JsonProperty(value="format") private var _format: String = "csv"
    @JsonProperty(value="partitions") private var _partitions: Seq[PartitionField] = _
    @JsonProperty(value="filename") private var _filename: String = _

    def filename(implicit context:Context) : String = context.evaluate(_filename)
    def location(implicit context:Context) : String = context.evaluate(_location)
    def format(implicit context:Context) : String = context.evaluate(_format)
    def partitions : Seq[PartitionField] = _partitions

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema     - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(executor: Executor, schema: StructType, partitions: Map[String, FieldValue]): DataFrame = {
        implicit val context = executor.context
        val inputFiles = collectFiles(executor, partitions)

        val reader = executor.spark.readLocal.options(options)
        if (this.schema != null)
            reader.schema(inputSchema)

        val rawData = reader
            .format(format)
            .load(inputFiles.map(_.toUri.getPath):_*)

        SchemaUtils.applySchema(rawData, schema)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param executor
      * @param df        - dataframe to write
      * @param partition - destination partition
      */
    override def write(executor: Executor, df: DataFrame, partition: Map[String, SingleValue], mode: String): Unit = {
        implicit val context = executor.context

        val outputPath  = collector(executor).resolve(partition.mapValues(_.value)).toUri.getPath

        logger.info(s"Writing to local output location '$outputPath' (partition=$partition)")

        // Create correct schema for output
        val outputColumns = schema.fields.map(field => df(field.name).cast(field.sparkType))
        val outputDf = df.select(outputColumns:_*)
        val writer = outputDf.writeLocal.options(options)

        writer.format(format)
            .mode(mode)
            .save(outputPath.toString)
    }

    /**
      * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
      * relation will not contain any data, but all metadata will be processed
      *
      * @param executor
      */
    override def create(executor: Executor): Unit =  {
        implicit val context = executor.context
        logger.info(s"Creating local directory '$localLocation' for local file relation")
        val path = new File(localLocation)
        path.mkdirs()
    }

    /**
      * This will delete any physical representation of the relation. Depending on the type only some meta data like
      * a Hive table might be dropped or also the physical files might be deleted
      *
      * @param executor
      */
    override def destroy(executor: Executor): Unit = {
        implicit val context = executor.context
        logger.info(s"Removing local directory '$localLocation' of local file relation")
        val root = new File(localLocation)

        def delete(file:File): Unit = {
            if (file.exists()) {
                if (file.isDirectory)
                    file.listFiles().foreach(delete)
                file.delete()
            }
        }

        delete(root)
    }

    /**
      * This will update any existing relation to the specified metadata.
      *
      * @param executor
      */
    override def migrate(executor: Executor): Unit = ???

    /**
      * Collects files for a given time period using the pattern inside the specification
      *
      * @param executor
      * @param partitions
      * @return
      */
    private def collectFiles(executor: Executor, partitions:Map[String,FieldValue]) : Seq[Path] = {
        implicit val context = executor.context
        if (location == null || location.isEmpty)
            throw new IllegalArgumentException("location needs to be defined for reading files")

        val inputFiles =
            if (this.partitions != null && this.partitions.nonEmpty)
                collectPartitionedFiles(executor, partitions)
            else
                collectUnpartitionedFiles(executor)

        // Print all files that we found
        inputFiles.foreach(f => logger.info("Reading input file {}", f.toString))
        inputFiles
    }

    private def collectPartitionedFiles(executor: Executor, partitions:Map[String,FieldValue]) : Seq[Path] = {
        implicit val context = executor.context
        if (partitions == null)
            throw new NullPointerException("Partitioned data source requires partition values to be defined")
        if (filename == null || filename.isEmpty)
            throw new IllegalArgumentException("pattern needs to be defined for reading partitioned files")

        val partitionColumnsByName = this.partitions.map(kv => (kv.name,kv)).toMap
        val resolvedPartitions = partitions.map(kv => (kv._1, partitionColumnsByName(kv._1).interpolate(kv._2)))
        collector(executor).collect(resolvedPartitions)
    }

    private def collectUnpartitionedFiles(executor: Executor) : Seq[Path] = {
        collector(executor).collect()
    }

    private def collector(executor: Executor) = {
        implicit val context = executor.context
        val path = new Path("file:///" + localLocation)
        new FileCollector(executor.spark)
            .path(path)
            .pattern(filename)
    }

    private def localLocation(implicit context: Context) = new Path(location).toUri.getPath
}
