/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.AbstractInstance
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.types.StructType


object MappingDataset {
    def apply(context: Context, mapping:MappingOutputIdentifier) : MappingDataset = {
        new MappingDataset(
            Dataset.Properties(context),
            mapping
        )
    }
}
case class MappingDataset(
    instanceProperties: Dataset.Properties,
    mapping: MappingOutputIdentifier
) extends AbstractInstance with Dataset {
    /**
      * Returns a list of physical resources produced by writing to this dataset
      * @return
      */
    override def provides : Set[ResourceIdentifier] = Set()

    /**
     * Returns a list of physical resources required for reading from this dataset
     * @return
     */
    override def requires : Set[ResourceIdentifier] = MappingUtils.requires(context, mapping.mapping)

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
    override def read(execution: Execution): DataFrame = {
        val instance = context.getMapping(mapping.mapping)
        execution.instantiate(instance, mapping.output)
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
    override def describe(execution:Execution) : Option[StructType] = {
        val instance = context.getMapping(mapping.mapping)
        Some(execution.describe(instance, mapping.output))
    }
}



class MappingDatasetSpec extends DatasetSpec {
    @JsonProperty(value="mapping", required = true) private var mapping: String = _

    override def instantiate(context: Context, properties:Option[Dataset.Properties] = None): MappingDataset = {
        val id = MappingOutputIdentifier(context.evaluate(mapping))
        MappingDataset(
            instanceProperties(context, id.toString, properties),
            id
        )
    }
}
