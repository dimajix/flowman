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
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.slf4j.LoggerFactory

import com.dimajix.common.Trilean
import com.dimajix.flowman.catalog.Catalog
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.MigrationFailedException
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.sql.SchemaUtils
import com.dimajix.spark.sql.SqlParser
import com.dimajix.spark.sql.catalyst.SqlBuilder


case class HiveViewRelation(
    override val instanceProperties:Relation.Properties,
    override val database: Option[String],
    override val table: String,
    override val partitions: Seq[PartitionField] = Seq(),
    sql: Option[String] = None,
    mapping: Option[MappingOutputIdentifier] = None
) extends HiveRelation {
    protected override val logger = LoggerFactory.getLogger(classOf[HiveViewRelation])

    /**
      * Returns the list of all resources which will be created by this relation. The list will be specifically
      * created for a specific partition, or for the full relation (when the partition is empty)
      *
      * @param partition
      * @return
      */
    override def resources(partition: Map[String, FieldValue]): Set[ResourceIdentifier] = Set()

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides : Set[ResourceIdentifier] = Set(
        ResourceIdentifier.ofHiveTable(table, database)
    )

    /**
      * Returns the list of all resources which will be required by this relation for creation.
      *
      * @return
      */
    override def requires : Set[ResourceIdentifier] = {
        val db = database.map(db => ResourceIdentifier.ofHiveDatabase(db)).toSet
        val other = mapping.map(m => MappingUtils.requires(context, m.mapping))
            .orElse(sql.map(s => SqlParser.resolveDependencies(s).map(t => ResourceIdentifier.ofHiveTable(t))))
            .getOrElse(Set())
        db ++ other
    }

    override def write(execution:Execution, df:DataFrame, partition:Map[String,SingleValue], mode:OutputMode) : Unit = {
        throw new UnsupportedOperationException()
    }

    /**
      * Truncating a view actually is non-op
      * @param execution
      * @param partitions
      */
    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
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
    override def loaded(execution: Execution, partition: Map[String, SingleValue]): Trilean =  {
        exists(execution)
    }

    /**
     * This method will physically create the corresponding Hive view
     *
     * @param execution
     */
    override def create(execution:Execution, ifNotExists:Boolean=false) : Unit = {
        val select = getSelect(execution)
        val catalog = execution.catalog
        if (!ifNotExists || !catalog.tableExists(tableIdentifier)) {
            logger.info(s"Creating Hive view relation '$identifier' with VIEW $tableIdentifier")
            catalog.createView(tableIdentifier, select, ifNotExists)
        }
    }

    /**
     * This will update any existing Hive view to the current definition. The update will only be performed, if the
     * definition actually changed.
     * @param execution
     */
    override def migrate(execution:Execution, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy) : Unit = {
        val catalog = execution.catalog
        if (catalog.tableExists(tableIdentifier)) {
            val newSelect = getSelect(execution)
            val curTable = catalog.getTable(tableIdentifier)
            // Check if current table is a VIEW or a table
            if (curTable.tableType == CatalogTableType.VIEW) {
                migrateFromView(catalog, newSelect, migrationStrategy)
            }
            else {
                migrateFromTable(catalog, newSelect, migrationStrategy)
            }
        }
    }

    private def migrateFromView(catalog:Catalog, newSelect:String, migrationStrategy:MigrationStrategy) : Unit = {
        val curTable = catalog.getTable(tableIdentifier)
        val curSchema = SchemaUtils.normalize(curTable.schema)
        val newSchema = SchemaUtils.normalize(catalog.spark.sql(newSelect).schema)
        if (curTable.viewText.get != newSelect || curSchema != newSchema) {
            migrationStrategy match {
                case MigrationStrategy.NEVER =>
                    logger.warn(s"Migration required for HiveView relation '$identifier' of Hive view $tableIdentifier, but migrations are disabled.")
                case MigrationStrategy.FAIL =>
                    logger.error(s"Cannot migrate relation HiveView '$identifier' of Hive view $tableIdentifier, since migrations are disabled.")
                    throw new MigrationFailedException(identifier)
                case MigrationStrategy.ALTER|MigrationStrategy.ALTER_REPLACE|MigrationStrategy.REPLACE =>
                    logger.info(s"Migrating HiveView relation '$identifier' with VIEW $tableIdentifier")
                    catalog.alterView(tableIdentifier, newSelect)
            }
        }
    }

    private def migrateFromTable(catalog:Catalog, newSelect:String, migrationStrategy:MigrationStrategy) : Unit = {
        migrationStrategy match {
            case MigrationStrategy.NEVER =>
                logger.warn(s"Migration required for HiveView relation '$identifier' from TABLE to a VIEW $tableIdentifier, but migrations are disabled.")
            case MigrationStrategy.FAIL =>
                logger.error(s"Cannot migrate relation HiveView '$identifier' from TABLE to a VIEW $tableIdentifier, since migrations are disabled.")
                throw new MigrationFailedException(identifier)
            case MigrationStrategy.ALTER|MigrationStrategy.ALTER_REPLACE|MigrationStrategy.REPLACE =>
                logger.info(s"Migrating HiveView relation '$identifier' from TABLE to a VIEW $tableIdentifier")
                catalog.dropTable(tableIdentifier, false)
                catalog.createView(tableIdentifier, newSelect, false)
        }
    }

    /**
     * This will drop the corresponding Hive view
     * @param execution
     */
    override def destroy(execution:Execution, ifExists:Boolean=false) : Unit = {
        val catalog = execution.catalog
        if (!ifExists || catalog.tableExists(tableIdentifier)) {
            logger.info(s"Destroying Hive view relation '$identifier' with VIEW $tableIdentifier")
            catalog.dropView(tableIdentifier)
        }
    }

    private def getSelect(executor: Execution) : String = {
        val select = sql.orElse(mapping.map(id => buildMappingSql(executor, id)))
            .getOrElse(throw new IllegalArgumentException("HiveView either requires explicit SQL SELECT statement or mapping"))

        logger.debug(s"Hive SQL SELECT statement for VIEW $tableIdentifier: $select")

        select
    }

    private def buildMappingSql(executor: Execution, output:MappingOutputIdentifier) : String = {
        val mapping = context.getMapping(output.mapping)
        val df = executor.instantiate(mapping, output.output)
        new SqlBuilder(df).toSQL
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
        HiveViewRelation(
            instanceProperties(context),
            context.evaluate(database),
            context.evaluate(view),
            partitions.map(_.instantiate(context)),
            context.evaluate(sql),
            context.evaluate(mapping).map(MappingOutputIdentifier.parse)
        )
    }
}
