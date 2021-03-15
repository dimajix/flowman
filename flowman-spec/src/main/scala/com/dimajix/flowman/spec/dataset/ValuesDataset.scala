/*
 * Copyright 2021 Kaya Kupferschmidt
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
import org.apache.spark.sql.DataFrame

import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.AbstractInstance
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.types.ArrayRecord
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.MapRecord
import com.dimajix.flowman.types.Record
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.ValueRecord
import com.dimajix.flowman.util.SchemaUtils
import com.dimajix.spark.sql.DataFrameUtils


case class ValuesDataset(
    instanceProperties: Dataset.Properties,
    columns:Seq[Field] = Seq(),
    schema:Option[Schema] = None,
    records:Seq[Record] = Seq()
) extends AbstractInstance with Dataset {
    override def provides: Set[ResourceIdentifier] = Set()

    /**
     * Returns a list of physical resources required for reading from this dataset
     *
     * @return
     */
    override def requires: Set[ResourceIdentifier] = Set()

    /**
     * Returns true if the data represented by this Dataset actually exists
     *
     * @param execution
     * @return
     */
    override def exists(execution: Execution): Trilean = Yes

    /**
     * Removes the data represented by this dataset, but leaves the underlying relation present
     *
     * @param execution
     */
    override def clean(execution: Execution): Unit = {
        throw new UnsupportedOperationException
    }

    /**
     * Reads data from the relation, possibly from specific partitions
     *
     * @param execution
     * @param schema - the schema to read. If none is specified, all available columns will be read
     * @return
     */
    override def read(execution: Execution, schema: Option[org.apache.spark.sql.types.StructType]): DataFrame = {
        val recordsSchema = StructType(this.schema.map(_.fields).getOrElse(columns))
        val sparkSchema = recordsSchema.sparkType

        val values = records.map(_.toArray(recordsSchema))
        val df = DataFrameUtils.ofStringValues(execution.spark, values, sparkSchema)
        SchemaUtils.applySchema(df, schema)
    }

    /**
     * Writes data into the relation, possibly into a specific partition
     *
     * @param execution
     * @param df - dataframe to write
     */
    override def write(execution: Execution, df: DataFrame, mode: OutputMode): Unit = {
        throw new UnsupportedOperationException
    }

    /**
     * Returns the schema as produced by this dataset, relative to the given input schema
     *
     * @return
     */
    override def describe(execution: Execution): Option[StructType] = {
        Some(StructType(schema.map(_.fields).getOrElse(columns)))
    }
}


class ValuesDatasetSpec extends DatasetSpec {
    @JsonProperty(value = "schema", required=false) private var schema:Option[SchemaSpec] = None
    @JsonProperty(value = "columns", required = false) private var columns:Map[String,String] = Map()
    @JsonProperty(value = "records", required=false) private var records:Seq[Record] = Seq()

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context): ValuesDataset = {
        val records = this.records.map {
            case v:ValueRecord => ValueRecord(context.evaluate(v.value))
            case a:ArrayRecord => ArrayRecord(a.fields.map(context.evaluate))
            case m:MapRecord => MapRecord(m.values.map(kv => kv._1 -> context.evaluate(kv._2)))
        }
        ValuesDataset(
            instanceProperties(context, "values"),
            context.evaluate(columns).toSeq.map(kv => Field(kv._1, FieldType.of(kv._2))),
            schema.map(_.instantiate(context)),
            records
        )
    }
}
