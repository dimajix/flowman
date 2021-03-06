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

package com.dimajix.flowman.spec.relation

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.model.SimpleResourceIdentifier
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.sql.SchemaUtils


class ProvidedRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema],
    val table:String
) extends BaseRelation with SchemaRelation {
    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides: Set[ResourceIdentifier] = Set()

    /**
      * Returns the list of all resources which will be required by this relation
      *
      * @return
      */
    override def requires: Set[ResourceIdentifier] = Set()


    /**
      * Returns the list of all resources which will are managed by this relation for reading or writing a specific
      * partition. The list will be specifically  created for a specific partition, or for the full relation (when the
      * partition is empty)
      *
      * @param partitions
      * @return
      */
    override def resources(partitions: Map[String, FieldValue]): Set[ResourceIdentifier] = Set(
        SimpleResourceIdentifier("provided", table)
    )

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param execution
      * @param schema
      * @param partitions
      * @return
      */
    override def read(execution:Execution, schema:Option[StructType], partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(execution != null)
        require(schema != null)
        require(partitions != null)

        val df = execution.spark.table(table)
        SchemaUtils.applySchema(df, schema)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param execution
      * @param df
      * @param partition
      */
    override def write(execution:Execution, df:DataFrame, partition:Map[String,SingleValue], mode:OutputMode) : Unit = {
        throw new UnsupportedOperationException(s"Writing into provided table '$table' not supported in relation '$identifier'")
    }

    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
        throw new UnsupportedOperationException(s"Truncating provided table '$table' not supported in relation '$identifier'")
    }


    /**
     * Returns true if the target partition exists and contains valid data. Absence of a partition indicates that a
     * [[write]] is required for getting up-to-date contents. A [[write]] with output mode
     * [[OutputMode.ERROR_IF_EXISTS]] then should not throw an error but create the corresponding partition
     *
     * @param execution
     * @param partition
     * @return
     */
    override def loaded(execution: Execution, partition: Map[String, SingleValue]): Trilean = {
        require(execution != null)
        require(partition != null)

        execution.spark.catalog.tableExists(table)
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
     *
     * @param execution
      * @return
      */
    override def exists(execution:Execution) : Trilean = {
        require(execution != null)

        execution.spark.catalog.tableExists(table)
    }

    override def create(execution: Execution, ifNotExists:Boolean=false): Unit = {
        if (!ifNotExists && exists(execution) == No)
            throw new UnsupportedOperationException(s"Cannot create provided table '$table' in relation '$identifier'")
    }

    override def destroy(execution: Execution, ifExists:Boolean=false): Unit = {
        if (!ifExists && exists(execution) == Yes)
            throw new UnsupportedOperationException(s"Cannot destroy provided table '$table' in relation '$identifier'")
    }

    override def migrate(execution: Execution, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy): Unit = {}
}




class ProvidedRelationSpec extends RelationSpec with SchemaRelationSpec {
    @JsonProperty(value="table") private var table: String = _

    override def instantiate(context: Context): Relation = {
        new ProvidedRelation(
            instanceProperties(context),
            schema.map(_.instantiate(context)),
            context.evaluate(table)
        )
    }
}
