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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.hadoop.FileCollector
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
import com.dimajix.flowman.util.SchemaUtils
import com.dimajix.flowman.util.UtcTimestamp


case class FileRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema],
    override val partitions: Seq[PartitionField],
    location:Path,
    pattern:Option[String],
    format:String
) extends BaseRelation with SchemaRelation with PartitionedRelation {
    private val logger = LoggerFactory.getLogger(classOf[FileRelation])

    private lazy val collector : FileCollector = {
        FileCollector.builder(context.hadoopConf)
            .path(location)
            .pattern(pattern)
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
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(executor:Executor, schema:Option[StructType], partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(executor != null)
        require(partitions != null)

        requireValidPartitionKeys(partitions)

        // Convert partition value to valid Spark literal
        def toLit(value:Any) : Column = value match {
            case v:UtcTimestamp => lit(v.toTimestamp())
            case _ => lit(value)
        }

        val data = mapFiles(partitions) { (partition, paths) =>
            paths.foreach(p => logger.info(s"Reading file relation '$identifier' partition ${HiveDialect.expr.partition(partition)} from location '$p' as '$format'"))

            val pathNames = paths.map(_.toString)
            val reader = this.reader(executor)
                .format(format)

            // Use either load(files) or load(single_file) - this actually results in different code paths in Spark
            // load(single_file) will set the "path" option, while load(multiple_files) needs direct support from the
            // underlying format implementation
            val providingClass = DataSource.lookupDataSource(format, executor.spark.sessionState.conf)
            val df = if (SparkShim.relationSupportsMultiplePaths(providingClass)) {
                reader.load(pathNames: _*)
            }
            else {
                reader.load(pathNames.mkString(","))
            }

            // Add partitions values as columns
            partition.toSeq.foldLeft(df)((df,p) => df.withColumn(p._1, toLit(p._2)))
        }
        val allData = data.reduce(_ union _)
        SchemaUtils.applySchema(allData, schema)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      * @param executor
      * @param df - dataframe to write
      * @param partition - destination partition
      */
    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:OutputMode = OutputMode.OVERWRITE) : Unit = {
        require(executor != null)
        require(partition != null)

        requireAllPartitionKeys(partition)

        val partitionSpec = PartitionSchema(partitions).spec(partition)
        val outputPath = collector.resolve(partitionSpec.toMap)

        logger.info(s"Writing file relation '$identifier' partition ${HiveDialect.expr.partition(partitionSpec)} to output location '$outputPath' as '$format'")

        this.writer(executor, df)
            .format(format)
            .mode(mode.batchMode)
            .save(outputPath.toString)
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      * @param executor
      * @return
      */
    override def exists(executor:Executor) : Boolean = {
        require(executor != null)

        val fs = location.getFileSystem(executor.spark.sparkContext.hadoopConfiguration)
        fs.exists(location)
    }

    /**
      * This method will create the given directory as specified in "location"
      * @param executor
      */
    override def create(executor:Executor, ifNotExists:Boolean=false) : Unit = {
        require(executor != null)

        val fs = location.getFileSystem(executor.spark.sparkContext.hadoopConfiguration)
        if (fs.exists(location)) {
            if (!ifNotExists) {
                throw new FileAlreadyExistsException(location.toString)
            }
        }
        else {
            logger.info(s"Creating file relation '$identifier' at location '$location'")
            fs.mkdirs(location)
        }
    }

    /**
      * This will update any existing relation to the specified metadata. Actually for this file based target, the
      * command will precisely do nothing.
      *
      * @param executor
      */
    override def migrate(executor:Executor) : Unit = {
    }

    /**
      * Removes one or more partitions.
      * @param executor
      * @param partitions
      */
    override def truncate(executor:Executor, partitions:Map[String,FieldValue] = Map()) : Unit = {
        require(executor != null)
        require(partitions != null)

        if (this.partitions.nonEmpty && partitions.nonEmpty)
            cleanPartitionedFiles(partitions)
        else
            cleanUnpartitionedFiles()
    }

    private def cleanPartitionedFiles(partitions:Map[String,FieldValue]) : Unit = {
        require(partitions != null)

        requireValidPartitionKeys(partitions)

        val resolvedPartitions = PartitionSchema(this.partitions).interpolate(partitions)
        collector.delete(resolvedPartitions)
    }

    private def cleanUnpartitionedFiles() : Unit = {
        collector.delete()
    }

    /**
      * This method will remove the given directory as specified in "location"
      * @param executor
      */
    override def destroy(executor:Executor, ifExists:Boolean) : Unit =  {
        require(executor != null)

        val fs = location.getFileSystem(executor.spark.sparkContext.hadoopConfiguration)
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
      * Collects files for a given time period using the pattern inside the specification
      *
      * @param partitions
      * @return
      */
    protected def mapFiles[T](partitions:Map[String,FieldValue])(fn:(PartitionSpec,Seq[Path]) => T) : Seq[T] = {
        require(partitions != null)

        if (this.partitions.nonEmpty)
            mapPartitionedFiles(partitions)(fn)
        else
            Seq(mapUnpartitionedFiles(fn))
    }

    private def mapPartitionedFiles[T](partitions:Map[String,FieldValue])(fn:(PartitionSpec,Seq[Path]) => T) : Seq[T] = {
        require(partitions != null)

        val resolvedPartitions = PartitionSchema(this.partitions).interpolate(partitions)
        resolvedPartitions.map(p => fn(p, collector.collect(p))).toSeq
    }

    private def mapUnpartitionedFiles[T](fn:(PartitionSpec,Seq[Path]) => T) : T = {
        fn(PartitionSpec(), collector.collect())
    }
}



class FileRelationSpec extends RelationSpec with SchemaRelationSpec with PartitionedRelationSpec {
    @JsonProperty(value="location", required = true) private var location: String = "/"
    @JsonProperty(value="format", required = true) private var format: String = "csv"
    @JsonProperty(value="pattern", required = false) private var pattern: Option[String] = None

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
            context.evaluate(format)
        )
    }
}
