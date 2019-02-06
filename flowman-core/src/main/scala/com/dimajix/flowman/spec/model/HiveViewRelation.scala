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
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


class HiveViewRelation extends SchemaRelation {
    private val logger = LoggerFactory.getLogger(classOf[HiveTableRelation])

    @JsonProperty(value="database") private var _database: String = _
    @JsonProperty(value="view") private var _view: String = _
    @JsonProperty(value="definition") private var _definition: String = _

    def database(implicit context:Context) : String = context.evaluate(_database)
    def view(implicit context:Context) : String = context.evaluate(_view)
    def definition(implicit context:Context) : String = context.evaluate(_definition)

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(executor:Executor, schema:StructType, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        implicit val context = executor.context
        val tableName = database + "." + view
        logger.info(s"Reading from Hive view $tableName")

        val reader = this.reader(executor)
        val df = reader.table(tableName)
        SchemaUtils.applySchema(df, schema)
    }

    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:String) : Unit = ???

    override def clean(executor: Executor, schema: StructType, partitions: Map[String, FieldValue]): Unit = {
        implicit val context = executor.context
        val tableName = database + "." + view
        logger.info(s"Cleaning from Hive view $tableName (no-op)")
    }

    override def create(executor:Executor) : Unit = ???
    override def destroy(executor:Executor) : Unit = ???
    override def migrate(executor:Executor) : Unit = ???
}
