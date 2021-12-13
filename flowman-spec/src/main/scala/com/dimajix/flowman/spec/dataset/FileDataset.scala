/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.dataset

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.slf4j.LoggerFactory

import com.dimajix.common.Trilean
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.AbstractInstance
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.sql.SchemaUtils


case class FileDataset(
    instanceProperties: Dataset.Properties,
    location:Path,
    format:String,
    options:Map[String,String] = Map(),
    schema:Option[Schema] = None
) extends AbstractInstance with Dataset {
    private val logger = LoggerFactory.getLogger(classOf[FileDataset])
    private val qualifiedPath = {
        val conf = context.hadoopConf
        val fs = location.getFileSystem(conf)
        location.makeQualified(fs.getUri, fs.getWorkingDirectory)
    }

    /**
      * Returns a list of physical resources produced by writing to this dataset
      * @return
      */
    override def provides : Set[ResourceIdentifier] = Set(
        ResourceIdentifier.ofFile(qualifiedPath)
    )

    /**
     * Returns a list of physical resources required for reading from this dataset
     * @return
     */
    override def requires : Set[ResourceIdentifier] = Set(
        ResourceIdentifier.ofFile(qualifiedPath)
    )

    /**
      * Returns true if the data represented by this Dataset actually exists
      *
      * @param execution
      * @return
      */
    override def exists(execution: Execution): Trilean = {
        val file = execution.fs.file(qualifiedPath)
        file.exists()
    }

    /**
      * Removes the data represented by this dataset, but leaves the underlying relation present
      *
      * @param execution
      */
    override def clean(execution: Execution): Unit = {
        require(execution != null)

        val file = execution.fs.file(qualifiedPath)
        if (file.exists()) {
            logger.info(s"Deleting directory '$qualifiedPath' of dataset '$name")
            file.delete( true)
        }
    }

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param execution
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @return
      */
    override def read(execution: Execution): DataFrame = {
        require(execution != null)

        val baseReader = execution.spark.read
            .options(options)
            .format(format)

        val reader = this.schema.map(s => baseReader.schema(s.sparkSchema)).getOrElse(baseReader)

        // Use either load(files) or load(single_file) - this actually results in different code paths in Spark
        // load(single_file) will set the "path" option, while load(multiple_files) needs direct support from the
        // underlying format implementation
        val providingClass = DataSource.lookupDataSource(format, execution.spark.sessionState.conf)
        val df = providingClass.getDeclaredConstructor().newInstance() match {
            case _: RelationProvider => reader.load(qualifiedPath.toString)
            case _: SchemaRelationProvider => reader.load(qualifiedPath.toString)
            case _: FileFormat => reader.load(Seq(qualifiedPath.toString):_*)
            case _ => reader.load(qualifiedPath.toString)
        }

        // Install callback to refresh DataFrame when data is overwritten
        execution.addResource(ResourceIdentifier.ofFile(qualifiedPath)) {
            df.queryExecution.logical.refresh()
        }

        df
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param execution
      * @param df - dataframe to write
      */
    override def write(execution: Execution, df: DataFrame, mode: OutputMode) : Unit = {
        val outputDf = SchemaUtils.applySchema(df, schema.map(_.sparkSchema))

        outputDf.write
            .options(options)
            .format(format)
            .mode(mode.batchMode)
            .save(qualifiedPath.toString)

        execution.refreshResource(ResourceIdentifier.ofFile(qualifiedPath))
    }

    /**
      * Returns the schema as produced by this dataset, relative to the given input schema
      *
      * @return
      */
    override def describe(execution:Execution) : Option[StructType] = {
        schema.map(s => StructType(s.fields))
    }
}



class FileDatasetSpec extends DatasetSpec {
    @JsonProperty(value="location", required = true) private var location: String = "/"
    @JsonProperty(value="format", required = false) private var format: String = "csv"
    @JsonProperty(value="options", required=false) private var options:Map[String,String] = Map()
    @JsonProperty(value="schema", required = false) protected var schema: Option[SchemaSpec] = None

    override def instantiate(context: Context): FileDataset = {
        val location = new Path(context.evaluate(this.location))
        FileDataset(
            instanceProperties(context, location.toString),
            location,
            context.evaluate(format),
            context.evaluate(options),
            schema.map(_.instantiate(context))
        )
    }
}
