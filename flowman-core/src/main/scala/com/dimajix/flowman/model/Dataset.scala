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

import com.dimajix.common.Trilean
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model
import com.dimajix.flowman.types.StructType


object Dataset {
    object Properties {
        def apply(context: Context, name:String = "", kind:String = "") : Properties = {
            Properties(
                context,
                Metadata(context, name, Category.DATASET, kind)
            )
        }
    }

    final case class Properties(
        context:Context,
        metadata:Metadata
    ) extends model.Properties[Properties] {
        require(metadata.category == Category.DATASET.lower)
        require(metadata.namespace == context.namespace.map(_.name))
        require(metadata.project == context.project.map(_.name))
        require(metadata.version == context.project.flatMap(_.version))

        override val namespace:Option[Namespace] = context.namespace
        override val project:Option[Project] = context.project
        override val kind : String = metadata.kind
        override val name : String  = metadata.name

        override def withName(name: String): Properties = copy(metadata=metadata.copy(name = name))

        def merge(other: Properties): Properties = {
            Properties(context, metadata.merge(other.metadata))
        }
    }
}


trait Dataset extends Instance {
    override type PropertiesType = Dataset.Properties

    /**
      * Returns the category of the resource
      *
      * @return
      */
    override final def category: Category = Category.DATASET

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
      * @param execution
      * @return
      */
    def exists(execution: Execution) : Trilean

    /**
      * Removes the data represented by this dataset, but leaves the underlying relation present
      * @param execution
      */
    def clean(execution: Execution) : Unit

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param execution
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @return
      */
    def read(execution:Execution) : DataFrame

    /**
      * Writes data into the relation, possibly into a specific partition
      * @param execution
      * @param df - dataframe to write
      */
    def write(execution:Execution, df:DataFrame, mode:OutputMode = OutputMode.OVERWRITE) : Unit

    /**
      * Returns the schema of this dataset that is either returned by [[read]] operations or that is expected
      * by [[write]] operations. If the schema is dynamic or cannot be inferred, [[None]] is returned.
      * @return
      */
    def describe(execution:Execution) : Option[StructType]
}
