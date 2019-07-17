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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.util.SchemaUtils


case class FileDataset(
    instanceProperties: Dataset.Properties,
    location:Path,
    format:String,
    options:Map[String,String],
    schema:Option[Schema]
) extends Dataset {
    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @return
      */
    override def read(executor: Executor, schema: Option[org.apache.spark.sql.types.StructType]): DataFrame = {
        require(executor != null)

        val baseReader = executor.spark.read
            .options(options)
            .format(format)

        val reader = this.schema.map(s => baseReader.schema(s.sparkSchema)).getOrElse(baseReader)

        // Use either load(files) or load(single_file) - this actually results in different code paths in Spark
        // load(single_file) will set the "path" option, while load(multiple_files) needs direct support from the
        // underlying format implementation
        val providingClass = DataSource.lookupDataSource(format, executor.spark.sessionState.conf)
        val df = providingClass.newInstance() match {
            case _: RelationProvider => reader.load(location.toString)
            case _: SchemaRelationProvider => reader.load(location.toString)
            case _: FileFormat => reader.load(Seq(location.toString):_*)
            case _ => reader.load(location.toString)
        }

        SchemaUtils.applySchema(df, schema)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param executor
      * @param df - dataframe to write
      */
    override def write(executor: Executor, df: DataFrame, mode: String) : Unit = {
        val outputDf = SchemaUtils.applySchema(df, schema.map(_.sparkSchema))

        outputDf.write
            .options(options)
            .format(format)
            .mode(mode)
            .save(location.toString)
    }

    /**
      * Returns the schema as produced by this dataset, relative to the given input schema
      *
      * @return
      */
    override def describe(): Option[StructType] = {
        schema.map(s => StructType(s.fields))
    }
}



class FileDatasetSpec extends DatasetSpec {
    @JsonProperty(value="location", required = true) private var location: String = "/"
    @JsonProperty(value="format", required = false) private var format: String = "csv"
    @JsonProperty(value="options", required=false) private var options:Map[String,String] = Map()
    @JsonProperty(value="schema", required = false) protected var schema: SchemaSpec = _

    override def instantiate(context: Context): FileDataset = {
        FileDataset(
            instanceProperties(context),
            new Path(context.evaluate(location)),
            context.evaluate(format),
            context.evaluate(options),
            Option(schema).map(_.instantiate(context))
        )
    }
}
