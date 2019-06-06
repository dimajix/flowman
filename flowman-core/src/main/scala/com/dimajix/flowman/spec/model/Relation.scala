/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.util.StdConverter
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.AbstractInstance
import com.dimajix.flowman.spec.Instance
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.spec.target.TargetSpec
import com.dimajix.flowman.spi.TypeRegistry
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue


object Relation {
    object Properties {
        def apply(context:Context, name:String="", kind:String="") : Properties = {
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                kind,
                Map(),
                "",
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
         labels:Map[String,String],
         description:String,
         options:Map[String,String]
    )
    extends Instance.Properties
}


/**
  * Interface class for declaring relations (for sources and sinks) as part of a model
  */
abstract class Relation extends AbstractInstance {
    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: String = "relation"

    /**
      * Returns an identifier for this relation
      * @return
      */
    def identifier : RelationIdentifier

    /**
      * Returns a description of the relation
      * @return
      */
    def description : String

    /**
      * Returns the Schema object which describes all fields of the relation
      * @return
      */
    def schema : Schema

    /**
      * Returns a list of fields
      * @return
      */
    def fields : Seq[Field] = schema.fields

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    def read(executor:Executor, schema:StructType, partitions:Map[String,FieldValue] = Map()) : DataFrame

    /**
      * Writes data into the relation, possibly into a specific partition
      * @param executor
      * @param df - dataframe to write
      * @param partition - destination partition
      */
    def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue] = Map(), mode:String = "OVERWRITE") : Unit

    /**
      * Removes one or more partitions.
      * @param executor
      * @param partitions
      */
    def clean(executor:Executor, partitions:Map[String,FieldValue] = Map()) : Unit

    /**
      * Reads data from a streaming source
      * @param executor
      * @param schema
      * @return
      */
    def readStream(executor:Executor, schema:StructType) : DataFrame = ???

    /**
      * Writes data to a streaming sink
      * @param executor
      * @param df
      * @return
      */
    def writeStream(executor:Executor, df:DataFrame, mode:OutputMode, checkpointLocation:Path) : StreamingQuery = ???

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      * @param executor
      * @return
      */
    def exists(executor:Executor) : Boolean

    /**
      * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
      * relation will not contain any data, but all metadata will be processed
      * @param executor
      */
    def create(executor:Executor, ifNotExists:Boolean=false) : Unit

    /**
      * This will delete any physical representation of the relation. Depending on the type only some meta data like
      * a Hive table might be dropped or also the physical files might be deleted
      * @param executor
      */
    def destroy(executor:Executor, ifExists:Boolean=false) : Unit

    /**
      * This will update any existing relation to the specified metadata.
      * @param executor
      */
    def migrate(executor:Executor) : Unit
}




object RelationSpec extends TypeRegistry[RelationSpec] {
    class NameResolver extends StdConverter[Map[String, RelationSpec], Map[String, RelationSpec]] {
        override def convert(value: Map[String, RelationSpec]): Map[String, RelationSpec] = {
            value.foreach(kv => kv._2.name = kv._1)
            value
        }
    }
}

/**
  * Interface class for declaring relations (for sources and sinks) as part of a model
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible=true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "jdbc", value = classOf[JdbcRelationSpec]),
    new JsonSubTypes.Type(name = "table", value = classOf[HiveTableRelationSpec]),
    new JsonSubTypes.Type(name = "view", value = classOf[HiveViewRelationSpec]),
    new JsonSubTypes.Type(name = "hiveTable", value = classOf[HiveTableRelationSpec]),
    new JsonSubTypes.Type(name = "hiveUnionView", value = classOf[HiveUnionViewRelationSpec]),
    new JsonSubTypes.Type(name = "hiveView", value = classOf[HiveViewRelationSpec]),
    new JsonSubTypes.Type(name = "file", value = classOf[FileRelationSpec]),
    new JsonSubTypes.Type(name = "local", value = classOf[LocalRelationSpec]),
    new JsonSubTypes.Type(name = "provided", value = classOf[ProvidedRelationSpec]),
    new JsonSubTypes.Type(name = "template", value = classOf[TemplateRelationSpec]),
    new JsonSubTypes.Type(name = "null", value = classOf[NullRelationSpec])
))
abstract class RelationSpec extends NamedSpec[Relation] {
    @JsonProperty(value="description", required = false) private var description: String = _
    @JsonProperty(value="options", required=false) private var options:Map[String,String] = Map()

    override def instantiate(context:Context) : Relation

    /**
      * Returns a set of common properties
      * @param context
      * @return
      */
    override protected def instanceProperties(context:Context) : Relation.Properties = {
        require(context != null)
        Relation.Properties(
            context,
            context.namespace,
            context.project,
            name,
            kind,
            context.evaluate(labels),
            context.evaluate(description),
            context.evaluate(options)
        )
    }
}
