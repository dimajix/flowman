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
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


case class HiveViewRelation(
    instanceProperties:Relation.Properties,
    database: String,
    view: String,
    definition: String
) extends BaseRelation {
    private val logger = LoggerFactory.getLogger(classOf[HiveTableRelation])

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(executor:Executor, schema:StructType, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(executor != null)
        require(partitions != null)

        val tableName = database + "." + view
        logger.info(s"Reading from Hive view $tableName")

        val reader = this.reader(executor)
        val df = reader.table(tableName)
        SchemaUtils.applySchema(df, schema)
    }

    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:String) : Unit = ???

    override def clean(executor: Executor, partitions: Map[String, FieldValue]): Unit = {
        require(executor != null)
        require(partitions != null)

        val tableName = database + "." + view
        logger.info(s"Cleaning from Hive view $tableName (no-op)")
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      * @param executor
      * @return
      */
    override def exists(executor:Executor) : Boolean = ???

    override def create(executor:Executor, ifNotExists:Boolean=false) : Unit = ???
    override def destroy(executor:Executor, ifExists:Boolean=false) : Unit = ???
    override def migrate(executor:Executor) : Unit = ???
}



class HiveViewRelationSpec extends RelationSpec {
    @JsonProperty(value="database") private var database: String = _
    @JsonProperty(value="view") private var view: String = _
    @JsonProperty(value="definition") private var definition: String = _

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): HiveViewRelation = {
        HiveViewRelation(
            instanceProperties(context),
            context.evaluate(database),
            context.evaluate(view),
            context.evaluate(definition)
        )
    }
}
