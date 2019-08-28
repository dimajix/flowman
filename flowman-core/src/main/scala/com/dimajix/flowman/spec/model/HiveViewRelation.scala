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
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.schema.PartitionField
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.sql.catalyst.SQLBuilder


class HiveViewRelation(
    override val instanceProperties:Relation.Properties,
    override val database: Option[String],
    override val table: String,
    override val partitions: Seq[PartitionField],
    val sql: Option[String],
    val mapping: Option[MappingOutputIdentifier]
) extends HiveRelation {
    protected override val logger = LoggerFactory.getLogger(classOf[HiveViewRelation])

    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:String) : Unit = ???

    override def clean(executor: Executor, partitions: Map[String, FieldValue]): Unit = ???

    override def create(executor:Executor, ifNotExists:Boolean=false) : Unit = {
        logger.info(s"Creating Hive view relation '$name' with VIEW name $tableIdentifier")

      val select = getSelect(executor)
      val catalog = executor.catalog
      if (!ifNotExists || !catalog.tableExists(tableIdentifier)) {
        catalog.createView(tableIdentifier, select, ifNotExists)
      }
    }

    override def destroy(executor:Executor, ifExists:Boolean=false) : Unit = {
        logger.info(s"Destroying Hive VIEW relation '$name' with VIEW $tableIdentifier")

        val catalog = executor.catalog
        if (!ifExists || catalog.tableExists(tableIdentifier)) {
            catalog.dropView(tableIdentifier)
        }
    }

    override def migrate(executor:Executor) : Unit = {
        logger.info(s"Migrating Hive VIEW relation $name with VIEW $tableIdentifier")

        val catalog = executor.catalog
        if (catalog.tableExists(tableIdentifier)) {
            catalog.dropView(tableIdentifier)
            val select = getSelect(executor)
            catalog.createView(tableIdentifier, select, false)
        }
    }

    private def getSelect(executor: Executor) : String = {
        val select = sql.orElse(mapping.map(id => buildMappingSql(executor, id)))
            .getOrElse(throw new IllegalArgumentException("HiveView either requires explicit SQL SELECT statement or mapping"))

        logger.debug(s"Hive SQL SELECT statement for VIEW $tableIdentifier: $sql")

        select
    }

    private def buildMappingSql(executor: Executor, output:MappingOutputIdentifier) : String = {
        val mapping = context.getMapping(output.mapping)
        val df = executor.instantiate(mapping, output.output)
        new SQLBuilder(df).toSQL
    }
}



class HiveViewRelationSpec extends RelationSpec with PartitionedRelationSpec{
    @JsonProperty(value="database", required = false) private var database: Option[String] = None
    @JsonProperty(value="view", required = true) private var view: String = _
    @JsonProperty(value="sql", required = false) private var sql: Option[String] = None
    @JsonProperty(value="mapping", required = false) private var mapping: Option[String] = None

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): HiveViewRelation = {
        new HiveViewRelation(
            instanceProperties(context),
            context.evaluate(database),
            context.evaluate(view),
            partitions.map(_.instantiate(context)),
            context.evaluate(sql),
            context.evaluate(mapping).map(MappingOutputIdentifier.parse)
        )
    }
}
