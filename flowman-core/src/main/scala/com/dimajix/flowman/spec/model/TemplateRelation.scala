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

package com.dimajix.flowman.spec.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.ScopeContext
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue


case class TemplateRelation(
       instanceProperties:Relation.Properties,
       relation:RelationIdentifier,
       environment:Map[String,String]
) extends BaseRelation {
    private val templateContext = ScopeContext.builder(context)
        .withEnvironment(environment)
        .build()
    private val relationInstance = {
        project.relations(relation.name).instantiate(templateContext)
    }

    /**
      * Returns a description for the relation
      * @return
      */
    override def description : String = relationInstance.description

    /**
      * Returns the schema of the relation
      * @return
      */
    override def schema : Schema = relationInstance.schema

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema     - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(executor: Executor, schema: Option[StructType], partitions: Map[String, FieldValue]): DataFrame = {
        require(executor != null)
        require(schema != null)
        require(partitions != null)

        relationInstance.read(executor, schema, partitions)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param executor
      * @param df        - dataframe to write
      * @param partition - destination partition
      */
    override def write(executor: Executor, df: DataFrame, partition: Map[String, SingleValue], mode: String): Unit = {
        require(executor != null)
        require(df != null)
        require(partition != null)

        relationInstance.write(executor, df, partition, mode)
    }

    /**
      * Removes one or more partitions.
      *
      * @param executor
      * @param partitions
      */
    override def clean(executor: Executor, partitions: Map[String, FieldValue]): Unit = {
        require(executor != null)

        relationInstance.clean(executor, partitions)
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      *
      * @param executor
      * @return
      */
    override def exists(executor: Executor): Boolean = {
        require(executor != null)

        relationInstance.exists(executor)
    }

    /**
      * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
      * relation will not contain any data, but all metadata will be processed
      *
      * @param executor
      */
    override def create(executor: Executor, ifNotExists: Boolean): Unit = {
        require(executor != null)

        relationInstance.create(executor, ifNotExists)
    }

    /**
      * This will delete any physical representation of the relation. Depending on the type only some meta data like
      * a Hive table might be dropped or also the physical files might be deleted
      *
      * @param executor
      */
    override def destroy(executor: Executor, ifExists: Boolean): Unit = {
        require(executor != null)

        relationInstance.destroy(executor, ifExists)
    }

    /**
      * This will update any existing relation to the specified metadata.
      *
      * @param executor
      */
    override def migrate(executor: Executor): Unit = {
        require(executor != null)

        relationInstance.migrate(executor)
    }
}




class TemplateRelationSpec extends RelationSpec {
    @JsonProperty(value = "relation", required = true) private var relation:String = _
    @JsonProperty(value = "environment", required = true) private var environment:Seq[String] = Seq()

    /**
      * Creates an instance of this specification and performs the interpolation of all variables
      *
      * @param context
      * @return
      */
    override def instantiate(context: Context): TemplateRelation = {
        TemplateRelation(
            instanceProperties(context),
            RelationIdentifier(context.evaluate(relation)),
            splitSettings(environment).toMap
        )
    }
}
