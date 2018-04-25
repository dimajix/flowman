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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.schema.Field


/**
  * Common base implementation for the Relation interface class. It contains a couple of common properties.
  */
abstract class BaseRelation extends Relation {
    @JsonProperty(value="schema", required=false) private var _schema: Seq[Field] = _
    @JsonProperty(value="description", required = false) private var _description: String = _
    @JsonProperty(value="options", required=false) private var _options:Map[String,String] = Map()
    @JsonProperty(value="defaults", required=false) private var _defaultValues:Map[String,String] = Map()

    override def description(implicit context: Context) : String = context.evaluate(_description)
    override def schema(implicit context: Context) : Seq[Field] = _schema
    def options(implicit context: Context) : Map[String,String] = _options.mapValues(context.evaluate)
    def defaultValues(implicit context: Context) : Map[String,String] = _defaultValues.mapValues(context.evaluate)

    /**
      * Creates a DataFrameReader which is already configured with options and the schema is also
      * already included
      * @param executor
      * @return
      */
    protected def reader(executor:Executor) : DataFrameReader = {
        implicit val context = executor.context
        val reader = executor.spark.read.options(options)
        if (schema != null)
            reader.schema(createSchema)
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
        val outputColumns = schema.map(field => df(field.name).cast(field.sparkType))
        val outputDf = df.select(outputColumns:_*)
        outputDf.write.options(options)
    }

    /**
      * Creates a Spark schema from the list of fields.
      * @param context
      * @return
      */
    protected def createSchema(implicit context:Context) : StructType = {
        StructType(schema.map(_.sparkField))
    }
}
