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

import java.nio.file.FileAlreadyExistsException
import java.nio.file.Paths

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import com.dimajix.common.Trilean
import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.fs.FileCollector
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
import com.dimajix.spark.sql.local.implicits._


final case class LocalRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema],
    override val partitions: Seq[PartitionField],
    location:File,
    pattern:Option[String],
    format:String = "csv",
    options:Map[String,String] = Map()
)
extends BaseRelation with SchemaRelation with PartitionedRelation {
    private lazy val collector : FileCollector = {
        FileCollector.builder(context.fs)
            .location(location)
            .pattern(pattern)
            .partitionBy(partitions.map(_.name):_*)
            .defaults(partitions.map(p => (p.name, "*")).toMap ++ context.environment.toMap)
            .build()
    }
    private lazy val qualifiedLocation = collector.root
    private lazy val resource = ResourceIdentifier.ofFile(qualifiedLocation)

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides(op:Operation, partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier] = {
        op match {
            case Operation.CREATE | Operation.DESTROY =>
                Set(resource)
            case Operation.READ => Set.empty
            case Operation.WRITE =>
                requireValidPartitionKeys(partitions)

                if (this.partitions.nonEmpty) {
                    val allPartitions = PartitionSchema(this.partitions).interpolate(partitions)
                    allPartitions.map(p => ResourceIdentifier.ofFile(collector.resolve(p).file)).toSet
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
                    allPartitions.map(p => ResourceIdentifier.ofFile(collector.resolve(p).file)).toSet
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
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(execution: Execution, partitions: Map[String, FieldValue]): DataFrame = {
        require(execution != null)
        require(partitions != null)

        requireValidPartitionKeys(partitions)

        // Convert partition value to valid Spark literal
        def toLit(value:Any) : Column = value match {
            case v:UtcTimestamp => lit(v.toTimestamp())
            case _ => lit(value)
        }

        logger.info(s"Reading local relation '$identifier' at '$qualifiedLocation' ${pattern.map(p => s" with pattern '$p'").getOrElse("")} for partitions (${partitions.map(kv => kv._1 + "=" + kv._2).mkString(", ")})")
        val data = mapFiles(partitions) { (partition, paths) =>
            logger.info(s"Local relation '$identifier' reads ${paths.size} files under location '${qualifiedLocation}' in partition ${partition.spec}")

            val reader = execution.spark.readLocal.options(options)
            inputSchema.foreach(s => reader.schema(s))

            val df = reader
                .format(format)
                .load(paths.map(p => Paths.get(p.uri)):_*)

            // Add partitions values as columns
            partition.toSeq.foldLeft(df)((df,p) => df.withColumn(p._1, toLit(p._2)))
        }

        val df1 = data.reduce(_ union _)

        // Add potentially missing partition columns
        val df2 = appendPartitionColumns(df1)

        applyInputSchema(execution, df2)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param execution
      * @param df        - dataframe to write
      * @param partition - destination partition
      */
    override def write(execution: Execution, df: DataFrame, partition: Map[String, SingleValue], mode: OutputMode): Unit = {
        require(execution != null)
        require(df != null)
        require(partition != null)

        requireAllPartitionKeys(partition)

        val outputPath  = collector.resolve(partition.mapValues(_.value))
        val outputFile = Paths.get(outputPath.uri)

        logger.info(s"Writing to local output location '$outputPath' (partition=$partition)")

        // Create correct schema for output
        val outputDf = applyOutputSchema(execution, df)
        val writer = outputDf.writeLocal.options(options)

        writer.format(format)
            .mode(mode.batchMode)
            .save(outputFile)

        execution.refreshResource(resource)
    }

    /**
     * Removes one or more partitions.
     * @param execution
     * @param partitions
     */
    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
        require(execution != null)
        require(partitions != null)

        java.lang.System.gc() // In Windows, open files may block truncation

        if (this.partitions.nonEmpty)
            truncatePartitionedFiles(partitions)
        else
            truncateUnpartitionedFiles()
    }

    private def truncatePartitionedFiles(partitions:Map[String,FieldValue]) : Unit = {
        require(partitions != null)

        requireValidPartitionKeys(partitions)

        val resolvedPartitions = PartitionSchema(this.partitions).interpolate(partitions)
        collector.delete(resolvedPartitions)
    }

    private def truncateUnpartitionedFiles() : Unit = {
        collector.truncate()
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      * @param execution
      * @return
      */
    override def exists(execution:Execution) : Trilean = {
        require(execution != null)

        localDirectory.exists()
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

        if(this.partitions.isEmpty) {
            val rootLocation = collector.resolve()
            rootLocation.file.exists()
        }
        else {
            val partitionSpec = PartitionSchema(partitions).spec(partition)
            collector.map(partitionSpec) { f =>
                f.glob().nonEmpty
            }
        }
    }

    /**
      * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
      * relation will not contain any data, but all metadata will be processed
      *
      * @param execution
      */
    override def create(execution: Execution): Unit =  {
        require(execution != null)

        if (localDirectory.exists()) {
            throw new FileAlreadyExistsException(qualifiedLocation.toString)
        }
        else {
            logger.info(s"Creating local directory '$localDirectory' for local file relation")
            localDirectory.mkdirs()
            execution.refreshResource(resource)
        }
    }

    /**
     * This will update any existing relation to the specified metadata. Actually for this file based target, the
     * command will precisely do nothing.
     *
     * @param execution
     */
    override def migrate(execution: Execution): Unit = {
    }

    /**
      * This will delete any physical representation of the relation. Depending on the type only some meta data like
      * a Hive table might be dropped or also the physical files might be deleted
      *
      * @param execution
      */
    override def destroy(execution: Execution): Unit = {
        require(execution != null)

        java.lang.System.gc() // In Windows, open files may block destruction

        val dir = localDirectory
        logger.info(s"Removing local directory '$dir' of local file relation")
        dir.delete(true)
        execution.refreshResource(resource)
    }

    /**
     * Collects files for a given time period using the pattern inside the specification
     *
     * @param partitions
     * @return
     */
    private def mapFiles[T](partitions:Map[String,FieldValue])(fn:(PartitionSpec,Seq[File]) => T) : Seq[T] = {
        require(partitions != null)

        if (this.partitions.nonEmpty)
            mapPartitionedFiles(partitions)(fn)
        else
            Seq(mapUnpartitionedFiles(fn))
    }

    private def mapPartitionedFiles[T](partitions:Map[String,FieldValue])(fn:(PartitionSpec,Seq[File]) => T) : Seq[T] = {
        require(partitions != null)

        val resolvedPartitions = PartitionSchema(this.partitions).interpolate(partitions)
        resolvedPartitions.map(p => fn(p, collector.glob(p))).toSeq
    }

    private def mapUnpartitionedFiles[T](fn:(PartitionSpec,Seq[File]) => T) : T = {
        fn(PartitionSpec(), collector.glob())
    }

    private def localDirectory = {
        if (collector.pattern.nonEmpty) {
            qualifiedLocation
        }
        else {
            qualifiedLocation.parent
        }
    }
}



class LocalRelationSpec extends RelationSpec with SchemaRelationSpec with PartitionedRelationSpec {
    @JsonProperty(value="location", required=true) private var location: String = "/"
    @JsonProperty(value="format", required=true) private var format: String = "csv"
    @JsonProperty(value="options", required=false) private var options:Map[String,String] = Map()
    @JsonProperty(value="pattern", required=false) private var pattern: Option[String] = None

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Relation.Properties] = None): LocalRelation = {
        LocalRelation(
            instanceProperties(context, properties),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            context.fs.file(context.evaluate(location)),
            pattern,
            context.evaluate(format),
            context.evaluate(options)
        )
    }
}
