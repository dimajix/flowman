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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


case class ProvidedRelation(
    instanceProperties:Relation.Properties,
    override val schema:Schema,
    table:String
) extends BaseRelation with SchemaRelation {
    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema
      * @param partitions
      * @return
      */
    override def read(executor:Executor, schema:Option[StructType], partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(executor != null)
        require(schema != null)
        require(partitions != null)

        val df = executor.spark.table(table)
        SchemaUtils.applySchema(df, schema)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param executor
      * @param df
      * @param partition
      */
    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:String) : Unit = {
        throw new UnsupportedOperationException("Writing into provided tables not supported")
    }

    override def clean(executor: Executor, partitions: Map[String, FieldValue]): Unit = {
        throw new UnsupportedOperationException("Cleaning provided tables not supported")
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      * @param executor
      * @return
      */
    override def exists(executor:Executor) : Boolean = {
        require(executor != null)

        executor.spark.catalog.tableExists(table)
    }

    override def create(executor: Executor, ifNotExists:Boolean=false): Unit = {
        throw new UnsupportedOperationException("Creating provided tables not supported")
    }
    override def destroy(executor: Executor, ifExists:Boolean=false): Unit = {
        throw new UnsupportedOperationException("Destroying provided tables not supported")
    }
    override def migrate(executor: Executor): Unit = {
        throw new UnsupportedOperationException("Migrating provided tables not supported")
    }

}




class ProvidedRelationSpec extends RelationSpec with SchemaRelationSpec {
    @JsonProperty(value="table") private var table: String = _

    override def instantiate(context: Context): Relation = {
        ProvidedRelation(
            instanceProperties(context),
            if (schema != null) schema.instantiate(context) else null,
            context.evaluate(table)
        )
    }
}
