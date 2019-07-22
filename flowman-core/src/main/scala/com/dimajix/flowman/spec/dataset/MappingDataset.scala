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
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.util.SchemaUtils


case class MappingDataset(
    instanceProperties: Dataset.Properties,
    mapping: MappingOutputIdentifier
) extends Dataset {
    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @return
      */
    override def read(executor: Executor, schema: Option[org.apache.spark.sql.types.StructType]): DataFrame = {
        val instance = context.getMapping(mapping.mapping)
        val df = executor.instantiate(instance, mapping.output)
        SchemaUtils.applySchema(df, schema)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param executor
      * @param df - dataframe to write
      */
    override def write(executor: Executor, df: DataFrame, mode: String): Unit = {
        throw new UnsupportedOperationException
    }

    /**
      * Returns the schema as produced by this dataset, relative to the given input schema
      *
      * @return
      */
    override def schema: Option[StructType] = {
        MappingUtils.describe(context, mapping)
    }
}



class MappingDatasetSpec extends DatasetSpec {
    @JsonProperty(value="mapping", required = true) private var mapping: String = _

    override def instantiate(context: Context): MappingDataset = {
        val id = MappingOutputIdentifier(context.evaluate(mapping))
        MappingDataset(
            instanceProperties(context, id.toString),
            id
        )
    }
}
