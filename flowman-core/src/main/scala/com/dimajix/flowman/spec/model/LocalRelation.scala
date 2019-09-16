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

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.hadoop.FileCollector
import com.dimajix.flowman.sources.local.implicits._
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.spec.schema.PartitionField
import com.dimajix.flowman.spec.schema.PartitionSchema
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


class LocalRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Schema,
    override val partitions: Seq[PartitionField],
    val location:Path,
    val pattern:String,
    val format:String
)
extends BaseRelation with SchemaRelation with PartitionedRelation {
    private val logger = LoggerFactory.getLogger(classOf[LocalRelation])

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides : Seq[ResourceIdentifier] = Seq(
        ResourceIdentifier.ofLocal(location)
    )

    /**
      * Returns the list of all resources which will be required by this relation
      *
      * @return
      */
    override def requires : Seq[ResourceIdentifier] = Seq()

    /**
      * Returns the list of all resources which will be required by this relation for reading a specific partition.
      * The list will be specifically  created for a specific partition, or for the full relation (when the partition
      * is empty)
      *
      * @param partitions
      * @return
      */
    override def resources(partitions: Map[String, FieldValue]): Seq[ResourceIdentifier] = {
        require(partitions != null)

        requireValidPartitionKeys(partitions)

        if (this.partitions.nonEmpty) {
            val allPartitions = PartitionSchema(this.partitions).interpolate(partitions)
            allPartitions.map(p => ResourceIdentifier.ofLocal(collector.resolve(p))).toSeq
        }
        else {
            Seq(ResourceIdentifier.ofLocal(location))
        }
    }

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema     - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(executor: Executor, schema: Option[StructType], partitions: Map[String, FieldValue]): DataFrame = {
        require(executor != null)
        require(schema != null)
        require(partitions != null)

        logger.info(s"Reading from local location '$location' (partitions=$partitions)")

        val inputFiles = collectFiles(partitions)
        val reader = executor.spark.readLocal.options(options)
        if (this.schema != null)
            reader.schema(inputSchema)

        val rawData = reader
            .format(format)
            .load(inputFiles.map(p => new File(p.toUri)):_*)

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
        require(executor != null)
        require(df != null)
        require(partition != null)

        val outputPath  = collector.resolve(partition.mapValues(_.value))
        val outputFile = new File(outputPath.toUri)

        logger.info(s"Writing to local output location '$outputPath' (partition=$partition)")

        // Create correct schema for output
        val outputDf = applyOutputSchema(df)
        val writer = outputDf.writeLocal.options(options)

        writer.format(format)
            .mode(mode)
            .save(outputFile)
    }

    override def truncate(executor: Executor, partitions: Map[String, FieldValue]): Unit = {
        require(executor != null)
        require(partitions != null)

        if (this.partitions != null && this.partitions.nonEmpty)
            cleanPartitionedFiles(partitions)
        else
            cleanUnpartitionedFiles()
    }

    private def cleanPartitionedFiles(partitions:Map[String,FieldValue]) = {
        if (pattern == null || pattern.isEmpty)
            throw new IllegalArgumentException("pattern needs to be defined for reading partitioned files")

        val resolvedPartitions = PartitionSchema(this.partitions).interpolate(partitions)
        collector.delete(resolvedPartitions)
    }

    private def cleanUnpartitionedFiles() = {
        collector.delete()
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      * @param executor
      * @return
      */
    override def exists(executor:Executor) : Boolean = {
        require(executor != null)

        new File(localDirectory).exists()
    }

    /**
      * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
      * relation will not contain any data, but all metadata will be processed
      *
      * @param executor
      */
    override def create(executor: Executor, ifNotExists:Boolean=false): Unit =  {
        require(executor != null)

        val dir = localDirectory
        logger.info(s"Creating local directory '$dir' for local file relation")
        val path = new File(dir)
        path.mkdirs()
    }

    /**
      * This will delete any physical representation of the relation. Depending on the type only some meta data like
      * a Hive table might be dropped or also the physical files might be deleted
      *
      * @param executor
      */
    override def destroy(executor: Executor, ifExists:Boolean=false): Unit = {
        require(executor != null)

        val dir = localDirectory
        logger.info(s"Removing local directory '$dir' of local file relation")
        val root = new File(dir)

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
      * @param partitions
      * @return
      */
    private def collectFiles(partitions:Map[String,FieldValue]) : Seq[Path] = {
        val inputFiles =
            if (this.partitions != null && this.partitions.nonEmpty)
                collectPartitionedFiles(partitions)
            else
                collectUnpartitionedFiles()

        // Print all files that we found
        inputFiles.foreach(f => logger.info("Reading input file {}", f.toString))
        inputFiles
    }

    private def collectPartitionedFiles(partitions:Map[String,FieldValue]) : Seq[Path] = {
        if (pattern == null || pattern.isEmpty)
            throw new IllegalArgumentException("pattern needs to be defined for reading partitioned files")

        val resolvedPartitions = PartitionSchema(this.partitions).interpolate(partitions)
        collector.collect(resolvedPartitions)
    }

    private def collectUnpartitionedFiles() : Seq[Path] = {
        collector.collect()
    }

    private def collector = {
        new FileCollector(context.hadoopConf)
            .path(location)
            .pattern(pattern)
    }

    private def localDirectory = {
        if (pattern != null && pattern.nonEmpty) {
            location.toUri.getPath
        }
        else {
            location.getParent.toUri.getPath
        }
    }
}



class LocalRelationSpec extends RelationSpec with SchemaRelationSpec with PartitionedRelationSpec {
    @JsonProperty(value="location", required=true) private var location: String = "/"
    @JsonProperty(value="format", required=false) private var format: String = "csv"
    @JsonProperty(value="pattern", required=false) private var pattern: String = _

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): LocalRelation = {
        new LocalRelation(
            instanceProperties(context),
            if (schema != null) schema.instantiate(context) else null,
            partitions.map(_.instantiate(context)),
            makePath(context.evaluate(location)),
            pattern,
            context.evaluate(format)
        )
    }

    private def makePath(location:String) : Path = {
        val path = new Path(location)
        if (path.isAbsoluteAndSchemeAuthorityNull)
            new Path("file", null, path.toString)
        else
            path
    }
}
