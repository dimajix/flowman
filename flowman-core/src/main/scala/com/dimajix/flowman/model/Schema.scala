/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.model.Dataset.Properties
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.StructType


object Schema {
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
    case class Properties(
        context:Context,
        namespace:Option[Namespace],
        project:Option[Project],
        name: String,
        kind: String,
        labels: Map[String, String]
    ) extends Instance.Properties[Properties] {
        override def withName(name: String): Properties = copy(name=name)
    }
}


/**
  * Interface class for declaring relations (for sources and sinks) as part of a model
  */
trait Schema extends Instance {
    /**
      * Returns the category of the resource
      *
      * @return
      */
    override final def category: Category = Category.SCHEMA

    /**
     * Returns a list of physical resources required by this schema
     * @return
     */
    def requires : Set[ResourceIdentifier]

    /**
      * Returns the description of the schema
 *
      * @return
      */
    def description : Option[String]

    /**
      * Returns the list of all fields of the schema
      * @return
      */
    def fields : Seq[Field]

    /**
      * Returns the list of primary keys. Can be empty of no PK is available
      * @return
      */
    def primaryKey : Seq[String]

    /**
      * Returns a Spark schema for this schema
      * @return
      */
    def sparkSchema : org.apache.spark.sql.types.StructType

    def catalogSchema : org.apache.spark.sql.types.StructType

    /**
      * Provides a human readable string representation of the schema
      */
    def printTree() : Unit = {
        println(treeString)
    }
    /**
      * Provides a human readable string representation of the schema
      */
    def treeString : String = {
        StructType(fields).treeString
    }
}


abstract class BaseSchema extends AbstractInstance with Schema {
    /**
     * Returns a list of physical resources required by this schema
     * @return
     */
    override def requires : Set[ResourceIdentifier] = Set()

    /**
     * Returns a Spark schema for this schema
     * @return
     */
    override def sparkSchema : org.apache.spark.sql.types.StructType = {
        org.apache.spark.sql.types.StructType(fields.map(_.sparkField))
    }

    override def catalogSchema : org.apache.spark.sql.types.StructType = {
        org.apache.spark.sql.types.StructType(fields.map(_.catalogField))
    }
}
