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
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.AbstractInstance
import com.dimajix.flowman.spec.Instance
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.spec.Spec
import com.dimajix.flowman.spec.target.Target.Properties
import com.dimajix.flowman.spi.TypeRegistry
import com.dimajix.flowman.types.StructType


object Dataset {
    object Properties {
        def apply(context:Context, name:String="", kind:String="") : Properties = {
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                kind,
                Map()
            )
        }
    }
    case class Properties(
        context:Context,
        namespace:Namespace,
        project:Project,
        name:String,
        kind:String,
        labels:Map[String,String]
    ) extends Instance.Properties
}


abstract class Dataset extends AbstractInstance {
    /**
      * Returns the category of the resource
      *
      * @return
      */
    override def category: String = "dataset"

    /**
      * Returns a list of physical resources produced by writing or reading to this dataset
      * @return
      */
    def resources : Set[ResourceIdentifier]

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
    def write(executor:Executor, df:DataFrame, mode:String = "OVERWRITE") : Unit

    /**
      * Returns the schema as produced by this dataset, relative to the given input schema
      * @return
      */
    def schema : Option[StructType]
}


object DatasetSpec extends TypeRegistry[DatasetSpec] {
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "file", value = classOf[FileDatasetSpec]),
    new JsonSubTypes.Type(name = "mapping", value = classOf[MappingDatasetSpec]),
    new JsonSubTypes.Type(name = "relation", value = classOf[RelationDatasetSpec])
))
abstract class DatasetSpec extends Spec[Dataset] {
    @JsonProperty(value="kind", required = true) protected var kind: String = _

    override def instantiate(context:Context) : Dataset

    /**
      * Returns a set of common properties
      * @param context
      * @return
      */
    protected def instanceProperties(context:Context, name:String) : Dataset.Properties = {
        require(context != null)
        Dataset.Properties(
            context,
            context.namespace,
            context.project,
            kind + "(" + name + ")",
            kind,
            Map()
        )
    }
}
