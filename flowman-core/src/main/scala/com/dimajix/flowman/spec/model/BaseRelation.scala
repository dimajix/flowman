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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.schema.PartitionField
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.util.SchemaUtils


/**
  * Common base implementation for the Relation interface class. It contains a couple of common properties.
  */
abstract class BaseRelation extends Relation {
    protected override def instanceProperties : Relation.Properties

    /**
      * Returns an identifier for this relation
      * @return
      */
    override def identifier : RelationIdentifier = RelationIdentifier(name, Option(project).map(_.name))

    /**
      * Returns a description for the relation
      * @return
      */
    override def description : Option[String] = instanceProperties.description

    /**
      * Returns the schema of the relation, excluding partition columns
      * @return
      */
    override def schema : Option[Schema] = None

    /**
      * Returns the list of partition columns
      * @return
      */
    override def partitions : Seq[PartitionField] = Seq()

    /**
      * Returns a map of all options. There is no specific usage for options, that depends on the
      * specific implementation
      * @return
      */
    def options : Map[String,String] = instanceProperties.options

    /**
      * Creates a DataFrameReader which is already configured with options and the schema is also
      * already included
      * @param executor
      * @return
      */
    protected def reader(executor:Executor) : DataFrameReader = {
        val reader = executor.spark.read.options(options)

        inputSchema.foreach(s => reader.schema(s))

        reader
    }

    /**
      * Creates a DataStreamReader which is already configured with options and the schema is also
      * already included
      * @param executor
      * @return
      */
    protected def streamReader(executor: Executor) : DataStreamReader = {
        val reader = executor.spark.readStream.options(options)

        inputSchema.foreach(s => reader.schema(s))

        reader
    }

    /**
      * Ceates a DataFrameWriter which is already configured with any options. Moreover
      * the desired schema of the relation is also applied to the DataFrame
      * @param executor
      * @param df
      * @return
      */
    protected def writer(executor: Executor, df:DataFrame) : DataFrameWriter[Row] = {
        val outputDf = applyOutputSchema(executor, df)
        outputDf.write.options(options)
    }

    /**
      * Ceates a DataStreamWriter which is already configured with any options. Moreover
      * the desired schema of the relation is also applied to the DataFrame
      * @param executor
      * @param df
      * @return
      */
    protected def streamWriter(executor: Executor, df:DataFrame, outputMode:OutputMode, checkpointLocation:Path) : DataStreamWriter[Row]= {
        val outputDf = applyOutputSchema(executor, df)
        outputDf.writeStream
            .options(options)
            .option("checkpointLocation", checkpointLocation.toString)
            .outputMode(outputMode)
    }

    /**
      * Creates a Spark schema from the list of fields.
      * @return
      */
    protected def inputSchema : Option[StructType] = {
        schema.map(s => StructType(s.fields.map(_.sparkField)))
    }

    /**
      * Creates a Spark schema from the list of fields. The list is used for output operations, i.e. for writing
      * @return
      */
    protected def outputSchema : Option[StructType] = {
        schema.map(s => s.sparkSchema)
    }

    /**
      * Applies the specified schema (or maybe even transforms it)
      * @param df
      * @return
      */
    protected def applyOutputSchema(executor:Executor, df:DataFrame) : DataFrame = {
        SchemaUtils.applySchema(df, outputSchema)
    }
}
