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
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StructType


case class RelationDataset(
    instanceProperties: Dataset.Properties,
    relation: RelationIdentifier,
    partition:Map[String,SingleValue]
) extends Dataset {
    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @return
      */
    override def read(executor: Executor, schema: Option[org.apache.spark.sql.types.StructType]): DataFrame = {
        val instance = context.getRelation(relation)
        instance.read(executor, schema, partition)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param executor
      * @param df - dataframe to write
      */
    override def write(executor: Executor, df: DataFrame, mode: String): Unit = {
        val instance = context.getRelation(relation)
        instance.write(executor, df, partition, mode)
    }

    /**
      * Returns the schema as produced by this dataset, relative to the given input schema
      *
      * @return
      */
    override def schema: Option[StructType] = {
        val instance = context.getRelation(relation)
        Some(StructType(instance.schema.fields))
    }
}


class RelationDatasetSpec extends DatasetSpec {
    @JsonProperty(value="relation", required = true) private var relation: String = _
    @JsonProperty(value="partition", required = false) private var partition: Map[String, String] = Map()

    override def instantiate(context: Context): RelationDataset = {
        val id = RelationIdentifier(context.evaluate(relation))
        RelationDataset(
            instanceProperties(context, id.toString),
            id,
            partition.map { case(n,p) => (n,SingleValue(context.evaluate(p))) }
        )
    }
}
