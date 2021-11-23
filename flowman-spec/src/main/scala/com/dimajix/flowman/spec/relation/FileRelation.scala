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

package com.dimajix.flowman.spec.relation

import java.io.FileNotFoundException
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileSystemException

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.hadoop.FileCollector
import com.dimajix.flowman.hadoop.FileUtils
import com.dimajix.flowman.jdbc.HiveDialect
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.UtcTimestamp
import com.dimajix.spark.sql.SchemaUtils


case class FileRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq(),
    location:Path,
    pattern:Option[String] = None,
    format:String = "csv",
    options:Map[String,String] = Map()
) extends BaseRelation with SchemaRelation with PartitionedRelation {
    private val logger = LoggerFactory.getLogger(classOf[FileRelation])

    private lazy val collector : FileCollector = {
        FileCollector.builder(context.hadoopConf)
            .path(location)
            .pattern(pattern)
            .partitionBy(partitions.map(_.name):_*)
            .defaults(partitions.map(p => (p.name, "*")).toMap ++ context.environment.toMap)
            .build()
    }

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides : Set[ResourceIdentifier] = Set(
        ResourceIdentifier.ofFile(location)
    )

    /**
      * Returns the list of all resources which will be required by this relation
      *
      * @return
      */
    override def requires : Set[ResourceIdentifier] = Set()

    /**
      * Returns the list of all resources which will be required by this relation for reading a specific partition.
      * The list will be specifically  created for a specific partition, or for the full relation (when the partition
      * is empty)
      *
      * @param partitions
      * @return
      */
    override def resources(partitions: Map[String, FieldValue]): Set[ResourceIdentifier] = {
        require(partitions != null)

        requireValidPartitionKeys(partitions)

        if (this.partitions.nonEmpty) {
            val allPartitions = PartitionSchema(this.partitions).interpolate(partitions)
            allPartitions.map(p => ResourceIdentifier.ofFile(collector.resolve(p))).toSet
        }
        else {
            Set(ResourceIdentifier.ofFile(location))
        }
    }

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param execution
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(execution:Execution, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(execution != null)
        require(partitions != null)

        requireValidPartitionKeys(partitions)

        // Convert partition value to valid Spark literal
        def toLit(value:Any) : Column = value match {
            case v:UtcTimestamp => lit(v.toTimestamp())
            case _ => lit(value)
        }

        logger.info(s"Reading file relation '$identifier' at '$location' ${pattern.map(p => s" with pattern '$p'").getOrElse("")} for partitions (${partitions.map(kv => kv._1 + "=" + kv._2).mkString(", ")})")
        val providingClass = DataSource.lookupDataSource(format, execution.spark.sessionState.conf)
        val multiPath =  SparkShim.relationSupportsMultiplePaths(providingClass)

        val data = mapFiles(partitions) { (partition, paths) =>
            logger.info(s"File relation '$identifier' reads ${paths.size} files under location '${location}' in partition ${partition.spec}")

            val pathNames = paths.map(_.toString)
            val reader = this.reader(execution, format, options)

            // Use either load(files) or load(single_file) - this actually results in different code paths in Spark
            // load(single_file) will set the "path" option, while load(multiple_files) needs direct support from the
            // underlying format implementation
            val df = if (multiPath) {
                reader.load(pathNames: _*)
            }
            else {
                reader.load(pathNames.mkString(","))
            }

            // Add partitions values as columns
            partition.toSeq.foldLeft(df)((df,p) => df.withColumn(p._1, toLit(p._2)))
        }

        val df = data.reduce(_ union _)

        // Install callback to refresh DataFrame when data is overwritten
        execution.addResource(ResourceIdentifier.ofFile(location)) {
            df.queryExecution.logical.refresh()
        }

        df
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      * @param execution
      * @param df - dataframe to write
      * @param partition - destination partition
      */
    override def write(execution:Execution, df:DataFrame, partition:Map[String,SingleValue], mode:OutputMode = OutputMode.OVERWRITE) : Unit = {
        require(execution != null)
        require(df != null)
        require(partition != null)

        if (partition.isEmpty && this.partitions.nonEmpty)
            doWriteDynamicPartitions(execution, df, mode)
        else
            doWriteStaticPartitions(execution, df, partition, mode)

        execution.refreshResource(ResourceIdentifier.ofFile(location))
    }
    private def doWriteDynamicPartitions(execution:Execution, df:DataFrame,  mode:OutputMode) : Unit = {
        val outputPath = collector.root
        logger.info(s"Writing file relation '$identifier' to output location '$outputPath' as '$format' with mode '$mode' with dynamic partitions")

        if (pattern.nonEmpty)
            throw new IllegalArgumentException(s"Pattern not supported for 'file' relation '$identifier' with dynamic partitions")

        mode match {
            // Since Flowman has a slightly different semantics of when data is available, we need to handle some
            // cases explicitly
            case OutputMode.IGNORE_IF_EXISTS =>
                if (loaded(execution) == No) {
                    doWriteDynamic(execution, df, outputPath, OutputMode.OVERWRITE)
                }
            case OutputMode.ERROR_IF_EXISTS =>
                if (loaded(execution) == Yes) {
                    throw new FileAlreadyExistsException(outputPath.toString)
                }
                doWriteDynamic(execution, df, outputPath, OutputMode.OVERWRITE)
            case m => m.batchMode
                doWriteDynamic(execution, df, outputPath, mode)
        }

    }
    private def doWriteDynamic(execution:Execution, df:DataFrame, outputPath:Path, mode:OutputMode) : Unit = {
        val overwriteMode = mode match {
            case OutputMode.OVERWRITE_DYNAMIC => "dynamic"
            case _ => "static"
        }
        this.writer(execution, df, format, options, mode.batchMode, dynamicPartitions=true)
            .option("partitionOverwriteMode", overwriteMode)
            .partitionBy(partitions.map(_.name):_*)
            .save(outputPath.toString)
    }
    private def doWriteStaticPartitions(execution:Execution, df:DataFrame, partition:Map[String,SingleValue], mode:OutputMode) : Unit = {
        val partitionSpec = PartitionSchema(partitions).spec(partition)
        val outputPath = collector.resolve(partitionSpec.toMap)
        logger.info(s"Writing file relation '$identifier' partition ${HiveDialect.expr.partition(partitionSpec)} to output location '$outputPath' as '$format' with mode '$mode'")

        requireAllPartitionKeys(partition)

        mode match {
            // Since Flowman has a slightly different semantics of when data is available, we need to handle some
            // cases explicitly
            case OutputMode.IGNORE_IF_EXISTS =>
                if (loaded(execution, partition) == No) {
                    doWrite(execution, df, outputPath, OutputMode.OVERWRITE)
                }
            case OutputMode.ERROR_IF_EXISTS =>
                if (loaded(execution, partition) == Yes) {
                    throw new FileAlreadyExistsException(outputPath.toString)
                }
                doWrite(execution, df, outputPath, OutputMode.OVERWRITE)
            case m => m.batchMode
                doWrite(execution, df, outputPath, mode)
        }
    }
    private def doWrite(execution:Execution, df:DataFrame, outputPath:Path, mode:OutputMode) : Unit = {
        this.writer(execution, df, format, options, mode.batchMode)
            .save(outputPath.toString)
    }


    /**
     * Reads data from a streaming source
     *
     * @param execution
     * @param schema
     * @return
     */
    override def readStream(execution: Execution): DataFrame = {
        logger.info(s"Streaming from file relation '$identifier' at '$location'")

        streamReader(execution, format, options).load(location.toString)
    }

    /**
     * Writes data to a streaming sink
     *
     * @param execution
     * @param df
     * @return
     */
    override def writeStream(execution: Execution, df: DataFrame, mode: OutputMode, trigger: Trigger, checkpointLocation: Path): StreamingQuery = {
        logger.info(s"Streaming to file relation '$identifier' at '$location'")

        if (pattern.nonEmpty)
            throw new IllegalArgumentException(s"Pattern not supported in streaming mode for 'file' relation '$identifier'")

        val writer = streamWriter(execution, df, format, options, mode.streamMode, trigger, checkpointLocation)
        if (partitions.nonEmpty) {
            writer
                .partitionBy(partitions.map(_.name):_*)
                .start(location.toString)
        }
        else {
            writer.start(location.toString)
        }
    }


    /**
     * Returns true if the target partition exists and contains valid data. Absence of a partition indicates that a
     * [[write]] is required for getting up-to-date contents. A [[write]] with output mode
     * [[OutputMode.ERROR_IF_EXISTS]] then should not throw an error but create the corresponding partition
     *
     * @param execution
     * @param partition
     * @return
     */
    override def loaded(execution: Execution, partition: Map[String, SingleValue]): Trilean = {
        require(execution != null)
        require(partition != null)

        requireValidPartitionKeys(partition)

        val rootLocation = collector.root
        val fs = rootLocation.getFileSystem(execution.hadoopConf)
        val isStream = FileUtils.isValidStreamData(fs, rootLocation)

        def checkPartition(path:Path) = {
            // TODO: Valid file data detection is difficult, since
            //  * streaming won't write a _SUCCESS file
            //  * dynamic partitioning writes a _SUCCESS file at the top level
            // FileUtils.isValidFileData(fs, path)
            FileUtils.isValidHiveData(fs, path)
        }

        if (this.partitions.nonEmpty) {
            val partitionSpec = PartitionSchema(partitions).spec(partition)
            collector.glob(partitionSpec).exists(checkPartition)
        }
        else {
            checkPartition(collector.resolve())
        }
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
     *
     * @param execution
      * @return
      */
    override def exists(execution:Execution) : Trilean = {
        require(execution != null)

        val fs = location.getFileSystem(execution.spark.sparkContext.hadoopConfiguration)
        fs.exists(location)
    }

    /**
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     *
     * @param execution
     * @return
     */
    override def conforms(execution: Execution, migrationPolicy: MigrationPolicy): Trilean = {
        exists(execution)
    }

    /**
      * This method will create the given directory as specified in "location"
      * @param execution
      */
    override def create(execution:Execution, ifNotExists:Boolean=false) : Unit = {
        require(execution != null)

        val fs = location.getFileSystem(execution.spark.sparkContext.hadoopConfiguration)
        if (fs.exists(location)) {
            if (!ifNotExists) {
                throw new FileAlreadyExistsException(location.toString)
            }
        }
        else {
            logger.info(s"Creating file relation '$identifier' at location '$location'")
            if (!fs.mkdirs(location)) {
                throw new FileSystemException(location.toString, "", "Cannot create directory.")
            }
        }
    }

    /**
      * This will update any existing relation to the specified metadata. Actually for this file based target, the
      * command will precisely do nothing.
      *
      * @param execution
      */
    override def migrate(execution:Execution, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy) : Unit = {
    }

    /**
      * Removes one or more partitions.
      * @param execution
      * @param partitions
      */
    override def truncate(execution:Execution, partitions:Map[String,FieldValue] = Map()) : Unit = {
        require(execution != null)
        require(partitions != null)

        requireValidPartitionKeys(partitions)

        if (this.partitions.nonEmpty) {
            truncatePartitionedFiles(partitions)
        }
        else {
            truncateUnpartitionedFiles()
        }
    }

    private def truncatePartitionedFiles(partitions:Map[String,FieldValue]) : Unit = {
        require(partitions != null)

        collector.delete(resolvePartitions(partitions))
    }

    private def truncateUnpartitionedFiles() : Unit = {
        collector.truncate()
    }

    /**
      * This method will remove the given directory as specified in "location"
      * @param execution
      */
    override def destroy(execution:Execution, ifExists:Boolean) : Unit =  {
        require(execution != null)

        val fs = location.getFileSystem(execution.spark.sparkContext.hadoopConfiguration)
        if (!fs.exists(location)) {
            if (!ifExists) {
                throw new FileNotFoundException(location.toString)
            }
        }
        else {
            logger.info(s"Destroying file relation '$identifier' by deleting directory '$location'")
            fs.delete(location, true)
        }
    }

    /**
      * Collects files for a given time period using the pattern inside the specification.
      *
      * @param partitions
      * @return
      */
    protected def mapFiles[T](partitions:Map[String,FieldValue])(fn:(PartitionSpec,Seq[Path]) => T) : Seq[T] = {
        require(partitions != null)

        if (this.partitions.nonEmpty) {
            val resolvedPartitions = resolvePartitions(partitions)
            if (resolvedPartitions.size > 2)
                resolvedPartitions.par.map(p => fn(p, collector.collect(p))).toList
            else
                resolvedPartitions.map(p => fn(p, collector.collect(p))).toSeq
        }
        else {
            Seq(fn(PartitionSpec(), collector.collect()))
        }
    }

    private def resolvePartitions(partitions:Map[String,FieldValue]) : Iterable[PartitionSpec] = {
        PartitionSchema(this.partitions).interpolate(partitions)
    }
}



class FileRelationSpec extends RelationSpec with SchemaRelationSpec with PartitionedRelationSpec {
    @JsonProperty(value="location", required = true) private var location: String = "/"
    @JsonProperty(value="format", required = true) private var format: String = "csv"
    @JsonProperty(value="pattern", required = false) private var pattern: Option[String] = None
    @JsonProperty(value="options", required=false) private var options:Map[String,String] = Map()

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): FileRelation = {
        FileRelation(
            instanceProperties(context),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            new Path(context.evaluate(location)),
            pattern,
            context.evaluate(format),
            context.evaluate(options)
        )
    }
}
