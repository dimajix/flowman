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

package com.dimajix.flowman.spec.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.hadoop.FileCollector
import com.dimajix.flowman.jdbc.HiveDialect
import com.dimajix.flowman.spec.schema.PartitionSchema
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


class FileRelation extends BaseRelation with SchemaRelation with PartitionedRelation {
    private val logger = LoggerFactory.getLogger(classOf[FileRelation])

    @JsonProperty(value="location", required = true) private var _location: String = "/"
    @JsonProperty(value="format", required = false) private var _format: String = "csv"
    @JsonProperty(value="pattern", required = false) private var _pattern: String = _

    def pattern(implicit context:Context) : String = context.evaluate(_pattern)
    def location(implicit context:Context) : Path = new Path(context.evaluate(_location))
    def format(implicit context:Context) : String = context.evaluate(_format)

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(executor:Executor, schema:StructType, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(executor != null)
        require(partitions != null)

        implicit val context = executor.context
        val data = mapFiles(executor, partitions) { (partition, paths) =>
            paths.foreach(p => logger.info(s"Reading from ${HiveDialect.expr.partition(partition)} file $p"))
            //if (inputFiles.isEmpty)
            //    throw new IllegalArgumentException("No input files found")

            val pathNames = paths.map(_.toString)
            val reader = this.reader(executor)
                .format(format)

            // Use either load(files) or load(single_file) - this actually results in different code paths in Spark
            // load(single_file) will set the "path" option, while load(multiple_files) needs direct support from the
            // underlying format implementation
            val providingClass = lookupDataSource(format, executor.spark.sessionState.conf)
            val df = providingClass.newInstance() match {
                case _: RelationProvider => reader.load(pathNames.mkString(","))
                case _: SchemaRelationProvider => reader.load(pathNames.mkString(","))
                case _: FileFormat => reader.load(pathNames: _*)
                case _ => reader.load(pathNames.mkString(","))
            }

            // Add partitions values as columns
            partition.toSeq.foldLeft(df)((df,p) => df.withColumn(p._1, lit(p._2)))
        }
        val allData = data.reduce(_ union _)
        SchemaUtils.applySchema(allData, schema)
    }

    private def lookupDataSource(provider: String, conf: SQLConf): Class[_] = {
        // Check appropriate method, depending on Spark version
        try {
            // Spark 2.2.x
            val method = DataSource.getClass.getDeclaredMethod("lookupDataSource", classOf[String])
            method.invoke(DataSource, provider).asInstanceOf[Class[_]]
        }
        catch {
            case _:NoSuchMethodException => {
                // Spark 2.3.x
                val method = DataSource.getClass.getDeclaredMethod("lookupDataSource", classOf[String], classOf[SQLConf])
                method.invoke(DataSource, provider, conf).asInstanceOf[Class[_]]
            }
        }
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      * @param executor
      * @param df - dataframe to write
      * @param partition - destination partition
      */
    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:String) : Unit = {
        require(executor != null)
        require(partition != null)

        implicit val context = executor.context

        val partitionSpec = PartitionSchema(partitions).spec(partition)
        val outputPath = collector(executor).resolve(partitionSpec.toMap)

        logger.info(s"Writing to output location '$outputPath' (partition=$partition) as '$format'")

        this.writer(executor, df)
            .format(format)
            .mode(mode)
            .save(outputPath.toString)
    }

    /**
      * Removes one or more partitions.
      * @param executor
      * @param partitions
      */
    override def clean(executor:Executor, partitions:Map[String,FieldValue] = Map()) : Unit = {
        require(executor != null)
        require(partitions != null)

        implicit val context = executor.context
        if (this.partitions != null && this.partitions.nonEmpty)
            cleanPartitionedFiles(executor, partitions)
        else
            cleanUnpartitionedFiles(executor)
    }

    private def cleanPartitionedFiles(executor: Executor, partitions:Map[String,FieldValue]) = {
        implicit val context = executor.context
        if (pattern == null || pattern.isEmpty)
            throw new IllegalArgumentException("pattern needs to be defined for reading partitioned files")

        val resolvedPartitions = PartitionSchema(this.partitions).interpolate(partitions)
        collector(executor).delete(resolvedPartitions)
    }

    private def cleanUnpartitionedFiles(executor: Executor) = {
        collector(executor).delete()
    }

    /**
      * This method will create the given directory as specified in "location"
      * @param executor
      */
    override def create(executor:Executor) : Unit = {
        implicit val context = executor.context
        val location = this.location
        logger.info(s"Creating directory '$location' for file relation")
        val fs = location.getFileSystem(executor.spark.sparkContext.hadoopConfiguration)
        fs.mkdirs(location)
    }

    /**
      * This method will remove the given directory as specified in "location"
      * @param executor
      */
    override def destroy(executor:Executor) : Unit =  {
        implicit val context = executor.context
        val location = this.location
        logger.info(s"Deleting directory '$location' of file relation")
        val fs = location.getFileSystem(executor.spark.sparkContext.hadoopConfiguration)
        fs.delete(location, true)
    }

    override def migrate(executor:Executor) : Unit = ???

    /**
      * Collects files for a given time period using the pattern inside the specification
      *
      * @param executor
      * @param partitions
      * @return
      */
    private def mapFiles[T](executor: Executor, partitions:Map[String,FieldValue])(fn:(PartitionSpec,Seq[Path]) => T) : Seq[T] = {
        implicit val context = executor.context
        if (this.partitions != null && this.partitions.nonEmpty)
            mapPartitionedFiles(executor, partitions)(fn)
        else
            Seq(mapUnpartitionedFiles(executor)(fn))
    }

    private def mapPartitionedFiles[T](executor: Executor, partitions:Map[String,FieldValue])(fn:(PartitionSpec,Seq[Path]) => T) : Seq[T] = {
        implicit val context = executor.context
        if (partitions == null)
            throw new NullPointerException("Partitioned data source requires partition values to be defined")
        if (pattern == null || pattern.isEmpty)
            throw new IllegalArgumentException("pattern needs to be defined for reading partitioned files")

        val collector = this.collector(executor)
        val resolvedPartitions = PartitionSchema(this.partitions).interpolate(partitions)
        resolvedPartitions.map(p => fn(p, collector.collect(p))).toSeq
    }

    private def mapUnpartitionedFiles[T](executor: Executor)(fn:(PartitionSpec,Seq[Path]) => T) : T = {
        fn(PartitionSpec(), collector(executor).collect())
    }

    private def collector(executor: Executor) = {
        implicit val context = executor.context
        new FileCollector(executor.spark)
            .path(location)
            .pattern(pattern)
    }
}
