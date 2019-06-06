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
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.schema.PartitionField
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue


case class HiveViewRelation(
    instanceProperties:Relation.Properties,
    override val database: String,
    override val table: String,
    definition: String,
    override val partitions: Seq[PartitionField]
) extends HiveRelation {
    protected override val logger = LoggerFactory.getLogger(classOf[HiveTableRelation])

    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:String) : Unit = ???

    override def clean(executor: Executor, partitions: Map[String, FieldValue]): Unit = ???

    override def create(executor:Executor, ifNotExists:Boolean=false) : Unit = ???

    override def destroy(executor:Executor, ifExists:Boolean=false) : Unit = {
        logger.info(s"Destroying Hive VIEW relation '$name' with table $tableIdentifier")

        val catalog = executor.catalog
        if (!ifExists || catalog.tableExists(tableIdentifier)) {
            catalog.dropView(tableIdentifier)
        }
    }

    override def migrate(executor:Executor) : Unit = ???
}



class HiveViewRelationSpec extends RelationSpec with PartitionedRelationSpec{
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
            context.evaluate(definition),
            partitions.map(_.instantiate(context))
        )
    }
}
