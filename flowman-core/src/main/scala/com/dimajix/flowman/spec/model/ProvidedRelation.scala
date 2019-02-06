/*
 * Copyright 2018 Kaya Kupferschmidt
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
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue


class ProvidedRelation extends SchemaRelation {
    @JsonProperty(value="table") private var _table: String = _

    def table(implicit context:Context) : String = context.evaluate(_table)

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema
      * @param partitions
      * @return
      */
    override def read(executor:Executor, schema:StructType, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        implicit val context = executor.context
        executor.spark.table(table)
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

    override def clean(executor: Executor, schema: StructType, partitions: Map[String, FieldValue]): Unit = {
        throw new UnsupportedOperationException("Cleaning provided tables not supported")
    }

    override def create(executor: Executor): Unit = {
        throw new UnsupportedOperationException("Creating provided tables not supported")
    }
    override def destroy(executor: Executor): Unit = {
        throw new UnsupportedOperationException("Destroying provided tables not supported")
    }
    override def migrate(executor: Executor): Unit = {
        throw new UnsupportedOperationException("Migrating provided tables not supported")
    }

}
