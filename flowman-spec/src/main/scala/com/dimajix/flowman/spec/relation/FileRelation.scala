/*
 * Copyright (C) 2018 The Flowman Authors
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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

import com.dimajix.common.MapIgnoreCase
import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.OutputMode.UPDATE
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.fs.FileCollector
import com.dimajix.flowman.fs.HadoopUtils
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


case class FileRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq(),
    location:Path,
    pattern:Option[String] = None,
    format:String = "csv",
    options:Map[String,String] = Map()
) extends BaseRelation with SchemaRelation with PartitionedRelation {
    private val resource = ResourceIdentifier.ofFile(qualifiedLocation)

    private lazy val collector : FileCollector = {
        FileCollector.builder(context.fs)
            .location(location)
            .pattern(pattern)
            .partitionBy(partitions.map(_.name):_*)
            .defaults(partitions.map(p => (p.name, "*")).toMap ++ context.environment.toMap)
            .build()
    }
    lazy val qualifiedLocation:File = collector.root

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides(op:Operation, partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier] = {
        op match {
            case Operation.CREATE | Operation.DESTROY => Set(resource)
            case Operation.READ => Set.empty
            case Operation.WRITE =>
                requireValidPartitionKeys(partitions)

                if (this.partitions.nonEmpty) {
                    val allPartitions = PartitionSchema(this.partitions).interpolate(partitions)
                    allPartitions.map(p => ResourceIdentifier.ofFile(collector.resolve(p).path)).toSet
                }
                else {
                    Set(resource)
                }
        }
    }

    /**
      * Returns the list of all resources which will be required by this relation
      *
      * @return
      */
    override def requires(op:Operation, partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier] = {
        val deps = op match {
            case Operation.CREATE | Operation.DESTROY => Set.empty
            case Operation.READ =>
                requireValidPartitionKeys(partitions)

                if (this.partitions.nonEmpty) {
                    val allPartitions = PartitionSchema(this.partitions).interpolate(partitions)
                    allPartitions.map(p => ResourceIdentifier.ofFile(collector.resolve(p).path)).toSet
                }
                else {
                    Set(resource)
                }
            case Operation.WRITE => Set.empty
        }
        deps ++ super.requires(op, partitions)
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

        logger.info(s"Reading file relation '$identifier' at '$qualifiedLocation' ${pattern.map(p => s" with pattern '$p'").getOrElse("")} for partitions (${partitions.map(kv => kv._1 + "=" + kv._2).mkString(", ")})")

        val useSpark = {
            if (this.partitions.isEmpty) {
                true
            }
            else if (this.pattern.nonEmpty) {
                false
            }
            else {
                val fs = location.getFileSystem(execution.hadoopConf)
                HadoopUtils.isPartitionedData(fs, location)
            }
        }

        // Add potentially missing partition columns
        val df =
            if (useSpark)
                readSpark(execution, partitions)
            else
                readCustom(execution, partitions)

        // Install callback to refresh DataFrame when data is overwritten
        execution.addResource(resource) {
            df.queryExecution.logical.refresh()
        }

        df
    }
    private def readCustom(execution:Execution, partitions:Map[String,FieldValue]) : DataFrame = {
        if (!collector.exists())
            throw new FileNotFoundException(s"Location '$qualifiedLocation' does match any existing directories and files")

        val providingClass = DataSource.lookupDataSource(format, execution.spark.sessionState.conf)
        val multiPath =  SparkShim.relationSupportsMultiplePaths(providingClass)

        val data = mapFiles(partitions) { (partition, paths) =>
            logger.info(s"Reading file relation '$identifier' at '${qualifiedLocation}' with partition ${partition.spec}, this will read files ${paths.mkString(",")}")

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
            partition.toSeq.foldLeft(df)((df,p) => df.withColumn(p._1, FieldValue.asLiteral(p._2)))
        }

        val df1 = data.reduce(_ union _)

        // Add potentially missing partition columns
        appendPartitionColumns(df1)
    }
    private def readSpark(execution:Execution, partitions:Map[String,FieldValue]) : DataFrame = {
        val reader = this.reader(execution, format, options)
        val reader1 = if (qualifiedLocation.isDirectory()) reader.option("basePath", qualifiedLocation.toString) else reader
        val df = reader1.load(qualifiedLocation.toString)

        // Filter partitions
        val parts = MapIgnoreCase(this.partitions.map(p => p.name -> p))
        partitions.foldLeft(df) { case(df,(pname, pvalue)) =>
            val part = parts(pname)
            df.filter(df(pname).isin(part.interpolate(pvalue).map(FieldValue.asLiteral).toSeq:_*))
        }
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

        execution.refreshResource(resource)
    }
    private def doWriteDynamicPartitions(execution:Execution, df:DataFrame,  mode:OutputMode) : Unit = {
        logger.info(s"Writing file relation '$identifier' to output location '$qualifiedLocation' as '$format' with mode '$mode' with dynamic partitions")

        if (pattern.nonEmpty)
            throw new IllegalArgumentException(s"Pattern not supported for 'file' relation '$identifier' with dynamic partitions")

        mode match {
            // Since Flowman has slightly different semantics of when data is available, we need to handle some
            // cases explicitly
            case OutputMode.IGNORE_IF_EXISTS =>
                if (loaded(execution) == No) {
                    doWriteDynamic(execution, df, OutputMode.OVERWRITE)
                }
            case OutputMode.ERROR_IF_EXISTS =>
                if (loaded(execution) == Yes) {
                    throw new FileAlreadyExistsException(qualifiedLocation.toString)
                }
                doWriteDynamic(execution, df, OutputMode.OVERWRITE)
            case OutputMode.APPEND|OutputMode.OVERWRITE|OutputMode.OVERWRITE_DYNAMIC =>
                doWriteDynamic(execution, df, mode)
            case _ =>
                throw new IllegalArgumentException(s"Unsupported output mode '$mode' for file relation $identifier")
        }

    }
    private def doWriteDynamic(execution:Execution, df:DataFrame, mode:OutputMode) : Unit = {
        val overwriteMode = mode match {
            case OutputMode.OVERWRITE_DYNAMIC => "dynamic"
            case _ => "static"
        }
        this.writer(execution, df, format, options, mode.batchMode, dynamicPartitions=true)
            .option("partitionOverwriteMode", overwriteMode)
            .partitionBy(partitions.map(_.name):_*)
            .save(qualifiedLocation.toString)

        // Manually add _SUCCESS file in case of OVERWRITE_DYNAMIC
        mode match {
            case OutputMode.OVERWRITE_DYNAMIC =>
                val success = qualifiedLocation / "_SUCCESS"
                if (!success.exists()) {
                    success.create().close()
                }
            case _ =>
        }
    }
    private def doWriteStaticPartitions(execution:Execution, df:DataFrame, partition:Map[String,SingleValue], mode:OutputMode) : Unit = {
        val partitionSpec = PartitionSchema(partitions).spec(partition)
        val partitionMap = partitionSpec.toMap
        val outputPath = collector.resolve(partitionMap)
        logger.info(s"Writing file relation '$identifier' partition ${partitionSpec.spec} to output location '$outputPath' as '$format' with mode '$mode'")

        requireAllPartitionKeys(partition)

        mode match {
            // Since Flowman has slightly different semantics of when data is available, we need to handle some
            // cases explicitly
            case OutputMode.IGNORE_IF_EXISTS =>
                if (loaded(execution, partition) == No) {
                    doWriteSinglePartition(execution, df, outputPath.path, OutputMode.OVERWRITE)
                }
            case OutputMode.ERROR_IF_EXISTS =>
                if (loaded(execution, partition) == Yes) {
                    throw new FileAlreadyExistsException(outputPath.toString)
                }
                doWriteSinglePartition(execution, df, outputPath.path, OutputMode.OVERWRITE)
            case OutputMode.APPEND|OutputMode.OVERWRITE|OutputMode.OVERWRITE_DYNAMIC =>
                doWriteSinglePartition(execution, df, outputPath.path, mode)
            case _ =>
                throw new IllegalArgumentException(s"Unsupported output mode '$mode' for file relation $identifier")
        }
    }
    private def doWriteSinglePartition(execution:Execution, df:DataFrame, outputPath:Path, mode:OutputMode) : Unit = {
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
        logger.info(s"Streaming from file relation '$identifier' at '$qualifiedLocation'")

        streamReader(execution, format, options).load(qualifiedLocation.toString)
    }

    /**
     * Writes data to a streaming sink
     *
     * @param execution
     * @param df
     * @return
     */
    override def writeStream(execution: Execution, df: DataFrame, mode: OutputMode, trigger: Trigger, checkpointLocation: Path): StreamingQuery = {
        logger.info(s"Streaming to file relation '$identifier' at '$qualifiedLocation'")

        if (pattern.nonEmpty)
            throw new IllegalArgumentException(s"Pattern not supported in streaming mode for 'file' relation '$identifier'")

        val writer = streamWriter(execution, df, format, options, mode.streamMode, trigger, checkpointLocation)
        if (partitions.nonEmpty) {
            writer
                .partitionBy(partitions.map(_.name):_*)
                .start(qualifiedLocation.toString)
        }
        else {
            writer.start(qualifiedLocation.toString)
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
        val fs = rootLocation.path.getFileSystem(execution.hadoopConf)

        def checkPartition(path:File) = {
            // streaming won't write a _SUCCESS file
            HadoopUtils.isValidFileData(fs, path.path, requireSuccessFile=true) ||
                HadoopUtils.isValidStreamData(fs, rootLocation.path)
        }
        def checkDirectory(path:File) = {
            // dynamic partitioning writes a _SUCCESS file at the top level
            HadoopUtils.isValidFileData(fs, path.path, requireSuccessFile=false)
        }

        if (this.partitions.nonEmpty) {
            val partitionSpec = PartitionSchema(partitions).spec(partition)
            // Check if root location contains a _SUCCESS file, then it might be dynamic partitioning
            if (pattern.isEmpty && checkPartition(rootLocation)) {
                collector.glob(partitionSpec).exists(checkDirectory)
            }
            else {
                collector.glob(partitionSpec).exists(checkPartition)
            }
        }
        else {
            //checkPartition(collector.resolve())
            checkPartition(rootLocation)
        }
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
     *
     * @param execution
      * @return
      */
    override def exists(execution:Execution) : Trilean = {
        collector.exists()
    }

    /**
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     *
     * @param execution
     * @return
     */
    override def conforms(execution: Execution): Trilean = {
        exists(execution)
    }

    /**
      * This method will create the given directory as specified in "location"
      * @param execution
      */
    override def create(execution:Execution) : Unit = {
        require(execution != null)

        if (collector.exists()) {
            throw new FileAlreadyExistsException(qualifiedLocation.toString)
        }
        else {
            logger.info(s"Creating file relation '$identifier' at location '$qualifiedLocation'")
            qualifiedLocation.mkdirs()
        }

        execution.refreshResource(resource)
    }

    /**
      * This will update any existing relation to the specified metadata. Actually for this file based target, the
      * command will precisely do nothing.
      *
      * @param execution
      */
    override def migrate(execution:Execution) : Unit = {
        // TODO: At least check partition changes
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
    override def destroy(execution:Execution) : Unit =  {
        require(execution != null)

        if (!collector.exists()) {
            throw new FileNotFoundException(qualifiedLocation.toString)
        }
        else {
            logger.info(s"Destroying file relation '$identifier' by deleting directory '$qualifiedLocation'")
            qualifiedLocation.delete(true)
        }

        execution.refreshResource(resource)
    }

    /**
      * Collects files for a given time period using the pattern inside the specification.
      *
      * @param partitions
      * @return
      */
    protected def mapFiles[T](partitions:Map[String,FieldValue])(fn:(PartitionSpec,Seq[File]) => T) : Seq[T] = {
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
    override def instantiate(context: Context, properties:Option[Relation.Properties] = None): FileRelation = {
        FileRelation(
            instanceProperties(context, properties),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            new Path(context.evaluate(location)),
            pattern,
            context.evaluate(format),
            context.evaluate(options)
        )
    }
}
