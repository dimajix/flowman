/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.common.Trilean
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.MigrationFailedException
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.fs.FileUtils
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.MigratableRelation
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.RegexResourceIdentifier
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.sql.SchemaUtils
import com.dimajix.spark.sql.SqlParser
import com.dimajix.spark.sql.catalyst.SqlBuilder


case class HiveViewRelation(
    override val instanceProperties:Relation.Properties,
    override val table: TableIdentifier,
    override val partitions: Seq[PartitionField] = Seq.empty,
    sql: Option[String] = None,
    mapping: Option[MappingOutputIdentifier] = None,
    file: Option[File] = None,
    override val migrationPolicy: MigrationPolicy = MigrationPolicy.RELAXED,
    override val migrationStrategy: MigrationStrategy = MigrationStrategy.ALTER
) extends HiveRelation with MigratableRelation {
    protected override val logger = LoggerFactory.getLogger(classOf[HiveViewRelation])
    private val resource = ResourceIdentifier.ofHiveTable(table)

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides(op:Operation, partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier] = {
        op match {
            case Operation.CREATE | Operation.DESTROY =>
                Set(resource)
            case Operation.READ =>
                requireValidPartitionKeys(partitions)

                val allPartitions = PartitionSchema(this.partitions).interpolate(partitions)
                allPartitions.map(p => ResourceIdentifier.ofHivePartition(table, p.toMap)).toSet
            case Operation.WRITE => Set.empty
        }
    }

    /**
      * Returns the list of all resources which will be required by this relation for creation.
      *
      * @return
      */
    override def requires(op:Operation, partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier] = {
        op match {
            case Operation.CREATE | Operation.DESTROY =>
                val db = table.database.map(db => ResourceIdentifier.ofHiveDatabase(db)).toSet
                val other = mapping.map { m =>
                        MappingUtils.requires(context, m.mapping)
                            // Replace all Hive partitions with Hive tables
                            .map {
                                case RegexResourceIdentifier("hiveTablePartition", table, _, _) => ResourceIdentifier.ofHiveTable(table)
                                case id => id
                            }
                    }
                    .orElse {
                        statement.map(s => SqlParser.resolveDependencies(s).map(t => ResourceIdentifier.ofHiveTable(t)))
                    }
                    .getOrElse(Set.empty)
                db ++ other
            case Operation.READ =>
                val other = mapping.map(m => MappingUtils.requires(context, m.mapping))
                    .orElse {
                        statement.map(s => SqlParser.resolveDependencies(s).flatMap(t => Set(
                            // Strictly speaking, we do not need to add the Hive table as a dependency, but this is
                            // more consistent with the result of MappingUtils.requires
                            ResourceIdentifier.ofHiveTable(t).asInstanceOf[ResourceIdentifier],
                            ResourceIdentifier.ofHivePartition(t, Map.empty[String, Any]).asInstanceOf[ResourceIdentifier])
                        ))
                    }
                    .getOrElse(Set.empty)
                other ++ Set(resource)
            case Operation.WRITE => Set.empty
        }
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
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     *
     * @param execution
     * @return
     */
    override def conforms(execution: Execution): Trilean = {
        val catalog = execution.catalog
        if (catalog.tableExists(table)) {
            val newSelect = getSelect(execution)
            val curTable = catalog.getTable(table)
            // Check if current table is a VIEW or a table
            if (curTable.tableType == CatalogTableType.VIEW) {
                // Check that both SQL and schema are correct
                lazy val curSchema = normalizeSchema(curTable.schema, migrationPolicy)
                lazy val newSchema = normalizeSchema(execution.spark.sql(newSelect).schema, migrationPolicy)
                curTable.viewText.contains(newSelect) && curSchema == newSchema
            }
            else {
                false
            }
        }
        else {
            false
        }
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
        exists(execution)
    }

    /**
     * This method will physically create the corresponding Hive view
     *
     * @param execution
     */
    override def create(execution:Execution) : Unit = {
        logger.info(s"Creating Hive view relation '$identifier' with VIEW $table")
        val catalog = execution.catalog
        val select = getSelect(execution)
        catalog.createView(table, select, false)
        execution.refreshResource(resource)
    }

    /**
     * This will update any existing Hive view to the current definition. The update will only be performed, if the
     * definition actually changed.
     * @param execution
     */
    override def migrate(execution:Execution) : Unit = {
        val catalog = execution.catalog
        if (catalog.tableExists(table)) {
            val newSelect = getSelect(execution)
            val curTable = catalog.getTable(table)
            // Check if current table is a VIEW or a TABLE
            if (curTable.tableType == CatalogTableType.VIEW) {
                migrateFromView(execution, curTable, newSelect)
            }
            else {
                migrateFromTable(execution, newSelect)
            }
        }
    }

    private def migrateFromView(execution:Execution, curTable:CatalogTable, newSelect:String) : Unit = {
        lazy val curSchema = normalizeSchema(curTable.schema, migrationPolicy)
        lazy val newSchema = normalizeSchema(execution.spark.sql(newSelect).schema, migrationPolicy)
        if (!curTable.viewText.contains(newSelect) || curSchema != newSchema) {
            migrationStrategy match {
                case MigrationStrategy.NEVER =>
                    logger.warn(s"Migration required for HiveView relation '$identifier' of Hive view $table, but migrations are disabled.")
                case MigrationStrategy.FAIL =>
                    logger.error(s"Cannot migrate relation HiveView '$identifier' of Hive view $table, since migrations are disabled.")
                    throw new MigrationFailedException(identifier)
                case MigrationStrategy.ALTER|MigrationStrategy.ALTER_REPLACE|MigrationStrategy.REPLACE =>
                    logger.info(s"Migrating HiveView relation '$identifier' with VIEW $table")
                    val catalog = execution.catalog
                    catalog.alterView(table, newSelect)
                    execution.refreshResource(resource)
            }
        }
    }

    private def migrateFromTable(execution:Execution, newSelect:String) : Unit = {
        migrationStrategy match {
            case MigrationStrategy.NEVER =>
                logger.warn(s"Migration required for HiveView relation '$identifier' from TABLE to a VIEW $table, but migrations are disabled.")
            case MigrationStrategy.FAIL =>
                logger.error(s"Cannot migrate relation HiveView '$identifier' from TABLE to a VIEW $table, since migrations are disabled.")
                throw new MigrationFailedException(identifier)
            case MigrationStrategy.ALTER|MigrationStrategy.ALTER_REPLACE|MigrationStrategy.REPLACE =>
                logger.info(s"Migrating HiveView relation '$identifier' from TABLE to a VIEW $table")
                val catalog = execution.catalog
                catalog.dropTable(table, false)
                catalog.createView(table, newSelect, false)
                execution.refreshResource(resource)
        }
    }

    /**
     * This will drop the corresponding Hive view
     * @param execution
     */
    override def destroy(execution:Execution) : Unit = {
        logger.info(s"Destroying Hive view relation '$identifier' with VIEW $table")
        val catalog = execution.catalog
        catalog.dropView(table)
        execution.refreshResource(resource)
    }

    private def getSelect(executor: Execution) : String = {
        val select = statement
            .orElse (
                mapping.map(id => buildMappingSql(executor, id))
            )
            .getOrElse(
                throw new IllegalArgumentException("HiveView either requires explicit SQL SELECT statement or mapping")
            )

        logger.debug(s"Hive SQL SELECT statement for VIEW $table: $select")

        select
    }

    private lazy val statement : Option[String] = {
        sql
            .orElse(file.map { f =>
                FileUtils.toString(f)
            })
    }

    private def normalizeSchema(schema:StructType, policy:MigrationPolicy) : StructType = {
        val normalized = SchemaUtils.normalize(schema)
        policy match {
            case MigrationPolicy.STRICT => normalized
            case MigrationPolicy.RELAXED => SchemaUtils.dropMetadata(normalized)
        }
    }

    private def buildMappingSql(executor: Execution, output:MappingOutputIdentifier) : String = {
        val mapping = context.getMapping(output.mapping)
        val df = executor.instantiate(mapping, output.output)
        new SqlBuilder(df).toSQL
    }
}



class HiveViewRelationSpec extends RelationSpec with PartitionedRelationSpec with MigratableRelationSpec {
    @JsonPropertyDescription("Name of the Hive database")
    @JsonProperty(value="database", required = false) private var database: Option[String] = None
    @JsonPropertyDescription("Name of the Hive view")
    @JsonProperty(value="view", required = true) private var view: String = _
    @JsonPropertyDescription("SQL query for the view definition. This has to be specified in Spark SQL syntax.")
    @JsonProperty(value="sql", required = false) private var sql: Option[String] = None
    @JsonProperty(value="mapping", required = false) private var mapping: Option[String] = None
    @JsonPropertyDescription("Name of a file containing the SQL query for the view definition. This has to be specified in Spark SQL syntax.")
    @JsonProperty(value="file", required=false) private var file:Option[String] = None

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Relation.Properties] = None): HiveViewRelation = {
        HiveViewRelation(
            instanceProperties(context, properties),
            TableIdentifier(context.evaluate(view), context.evaluate(database)),
            partitions.map(_.instantiate(context)),
            context.evaluate(sql),
            context.evaluate(mapping).map(MappingOutputIdentifier.parse),
            context.evaluate(file).map(p => context.fs.file(p)),
            evalMigrationPolicy(context),
            evalMigrationStrategy(context)
        )
    }
}
