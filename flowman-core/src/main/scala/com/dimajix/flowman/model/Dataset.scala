/*
 * Copyright 2019-2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.model

import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.types.StructType


object Dataset {
    object Properties {
        def apply(context: Context, name:String = "") : Properties = {
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                "",
                Map()
            )
        }
    }

    final case class Properties(
        context:Context,
        namespace:Option[Namespace],
        project:Option[Project],
        name:String,
        kind:String,
        labels:Map[String,String]
    ) extends Instance.Properties
}


trait Dataset extends Instance {
    /**
      * Returns the category of the resource
      *
      * @return
      */
    override def category: String = "dataset"

    /**
      * Returns a list of physical resources produced by writing to this dataset
      * @return
      */
    def provides : Set[ResourceIdentifier]

    /**
     * Returns a list of physical resources required for reading from this dataset
     * @return
     */
    def requires : Set[ResourceIdentifier]

    /**
      * Returns true if the data represented by this Dataset actually exists
      * @param executor
      * @return
      */
    def exists(executor: Executor) : Boolean

    /**
      * Removes the data represented by this dataset, but leaves the underlying relation present
      * @param executor
      */
    def clean(executor: Executor) : Unit

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @return
      */
    def read(executor:Executor, schema:Option[org.apache.spark.sql.types.StructType]) : DataFrame

    /**
      * Writes data into the relation, possibly into a specific partition
      * @param executor
      * @param df - dataframe to write
      */
    def write(executor:Executor, df:DataFrame, mode:OutputMode = OutputMode.OVERWRITE) : Unit

    /**
      * Returns the schema as produced by this dataset, relative to the given input schema
      * @return
      */
    def describe(executor:Executor) : Option[StructType]
}
