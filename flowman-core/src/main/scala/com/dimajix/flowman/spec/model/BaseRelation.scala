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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.schema.Schema


/**
  * Common base implementation for the Relation interface class. It contains a couple of common properties.
  */
abstract class BaseRelation extends Relation {
    @JsonProperty(value="description", required = false) private var _description: String = _
    @JsonProperty(value="options", required=false) private var _options:Map[String,String] = Map()

    /**
      * Returns a description for the relation
      * @param context
      * @return
      */
    override def description(implicit context: Context) : String = context.evaluate(_description)

    /**
      * Returns the schema of the relation
      * @param context
      * @return
      */
    override def schema(implicit context: Context) : Schema = null

    def options(implicit context: Context) : Map[String,String] = _options.mapValues(context.evaluate)

    /**
      * Creates a DataFrameReader which is already configured with options and the schema is also
      * already included
      * @param executor
      * @return
      */
    protected def reader(executor:Executor) : DataFrameReader = {
        implicit val context = executor.context
        val reader = executor.spark.read.options(options)

        val schema = this.inputSchema
        if (schema != null)
            reader.schema(schema)

        reader
    }

    /**
      * Creates a DataStreamReader which is already configured with options and the schema is also
      * already included
      * @param executor
      * @return
      */
    protected def streamReader(executor: Executor) : DataStreamReader = {
        implicit val context = executor.context
        val reader = executor.spark.readStream.options(options)

        val schema = this.inputSchema
        if (schema != null)
            reader.schema(schema)

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
        implicit val context = executor.context
        val outputDf = applyOutputSchema(df)
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
        implicit val context = executor.context
        val outputDf = applyOutputSchema(df)
        outputDf.writeStream
            .options(options)
            .option("checkpointLocation", checkpointLocation.toString)
            .outputMode(outputMode)
    }

    /**
      * Creates a Spark schema from the list of fields.
      * @param context
      * @return
      */
    protected def inputSchema(implicit context:Context) : StructType = {
        val schema = this.schema
        if (schema != null) {
            StructType(schema.fields.map(_.sparkField))
        }
        else {
            null
        }
    }

    /**
      * Creates a Spark schema from the list of fields. The list is used for output operations, i.e. for writing
      * @param context
      * @return
      */
    protected def outputSchema(implicit context:Context) : StructType = {
        val schema = this.schema
        if (schema != null) {
            StructType(schema.fields.map(_.sparkField))
        }
        else {
            null
        }
    }

    /**
      * Applies the specified schema (or maybe even transforms it)
      * @param df
      * @return
      */
    protected def applyOutputSchema(df:DataFrame)(implicit context:Context) : DataFrame = {
        val schema = this.outputSchema
        if (schema != null) {
            val outputColumns = schema.fields
                .map(field => df(field.name).cast(field.dataType).as(field.name, field.metadata))
            df.select(outputColumns: _*)
        }
        else {
            df
        }
    }
}
