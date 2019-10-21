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
import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.ExecutionException
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.spec.schema.PartitionField
import com.dimajix.flowman.spec.schema.PartitionSchema
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.transforms.SchemaEnforcer
import com.dimajix.flowman.transforms.UnionTransformer
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils
import com.dimajix.spark.sql.catalyst.SQLBuilder


class HiveUnionTableRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema],
    override val partitions: Seq[PartitionField],
    val tableDatabase: Option[String],
    val tablePrefix: String,
    val locationPrefix: Option[Path],
    val viewDatabase: Option[String],
    val view: String,
    val external: Boolean,
    val format: String,
    val rowFormat: Option[String],
    val inputFormat: Option[String],
    val outputFormat: Option[String],
    val properties: Map[String, String],
    val serdeProperties: Map[String, String]
)  extends BaseRelation with PartitionedRelation {
    private val logger = LoggerFactory.getLogger(classOf[HiveUnionTableRelation])

    def viewIdentifier: TableIdentifier = TableIdentifier(view, viewDatabase)
    def tableIdentifier: TableIdentifier = TableIdentifier(tablePrefix + "_*", tableDatabase)
    def tableIdentifier(version:Int) : TableIdentifier = {
        TableIdentifier(tablePrefix + "_" + version.toString, tableDatabase)
    }

    private def listTables(executor: Executor) : Seq[TableIdentifier] = {
        val catalog = executor.catalog
        val regex = (TableIdentifier(tablePrefix, tableDatabase).unquotedString + "_[0-9]+").r
        catalog.listTables(tableDatabase.getOrElse(catalog.currentDatabase), tablePrefix + "_*")
            .filter { table =>
                table.unquotedString match {
                    case regex() => true
                    case _ => false
                }
            }
    }

    private def tableRelation(version:Int) : HiveTableRelation =
        tableRelation(
            TableIdentifier(tablePrefix + "_" + version.toString, tableDatabase),
            locationPrefix.map(p => new Path(p.toString + "_" + version.toString))
        )

    private def tableRelation(tableIdentifier: TableIdentifier, location:Option[Path]) : HiveTableRelation = new HiveTableRelation(
        instanceProperties,
        schema,
        partitions,
        tableIdentifier.database,
        tableIdentifier.table,
        external,
        location,
        format,
        rowFormat,
        inputFormat,
        outputFormat,
        properties,
        serdeProperties,
        "hive"
    )

    private def viewRelationFromSql(sql:String) : HiveViewRelation = {
        new HiveViewRelation(
            instanceProperties,
            viewDatabase,
            view,
            partitions,
            Some(sql),
            None
        )
    }

    private def viewRelationFromTables(executor: Executor) : HiveViewRelation = {
        val tables = listTables(executor)
        val spark = executor.spark
        val df = tables.map(t => spark.read.table(t.unquotedString))
        val union = UnionTransformer().transformDataFrames(df)
        val finalSchema = schema.get.sparkSchema.fields ++ partitions.map(_.sparkField)
        val conformed = SchemaEnforcer(StructType(finalSchema)).transform(union)
        val sql = new SQLBuilder(conformed).toSQL
        viewRelationFromSql(sql)
    }

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides: Set[ResourceIdentifier] = Set(
        ResourceIdentifier.ofHiveTable(tablePrefix + "_*", tableDatabase),
        ResourceIdentifier.ofHiveTable(view, viewDatabase.orElse(tableDatabase))
    )

    /**
      * Returns the list of all resources which will be required by this relation for creation.
      *
      * @return
      */
    override def requires: Set[ResourceIdentifier] = {
        tableDatabase.map(db => ResourceIdentifier.ofHiveDatabase(db)).toSet ++
        viewDatabase.map(db => ResourceIdentifier.ofHiveDatabase(db)).toSet
    }

    /**
      * Returns the list of all resources which will are managed by this relation for reading or writing a specific
      * partition. The list will be specifically  created for a specific partition, or for the full relation (when the
      * partition is empty)
      *
      * @param partition
      * @return
      */
    override def resources(partition: Map[String, FieldValue]): Set[ResourceIdentifier] = {
        require(partitions != null)

        requireValidPartitionKeys(partition)

        val allPartitions = PartitionSchema(this.partitions).interpolate(partition)
        allPartitions.map(p => ResourceIdentifier.ofHivePartition(tablePrefix + "_*", tableDatabase, p.toMap)).toSet
    }


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

        logger.info(s"Reading from Hive union view '$viewIdentifier' using partition values $partitions")

        val tableDf = executor.spark.read.table(viewIdentifier.unquotedString)
        val df = filterPartition(tableDf, partitions)

        SchemaUtils.applySchema(df, schema)
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

        val catalog = executor.catalog
        val partitionSchema = PartitionSchema(this.partitions)

        // 1. Find all tables
        val allTables = listTables(executor)

        // 2. Find appropriate table
        val table = allTables.find { id =>
                val table = catalog.getTable(id)
                SchemaUtils.isCompatible(df.schema, table.schema)
            }
            .getOrElse(throw new ExecutionException(s"Cannot find appropriate target table for Hive Union Table '$identifier'"))

        // 3. Drop  partition from all other tables
        allTables.filter(_ != table).foreach { table =>
            catalog.dropPartition(table, partitionSchema.spec(partition), ignoreIfNotExists=true, purge=true)
        }

        // 4. Write to that table
        val relation = tableRelation(table, None)
        relation.write(executor, df, partition, "overwrite")
    }

    /**
      * Removes one or more partitions.
      *
      * @param executor
      * @param partitions
      */
    override def truncate(executor: Executor, partitions: Map[String, FieldValue]): Unit = {
        require(executor != null)

        listTables(executor)
            .foreach { table =>
                val relation = tableRelation(table, None)
                relation.truncate(executor, partitions)
            }
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      *
      * @param executor
      * @return
      */
    override def exists(executor: Executor): Boolean = {
        require(executor != null)

        val catalog = executor.catalog
        catalog.tableExists(viewIdentifier)
    }

    /**
      * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
      * relation will not contain any data, but all metadata will be processed
      *
      * @param executor
      */
    override def create(executor: Executor, ifNotExists: Boolean): Unit = {
        require(executor != null)

        if (!ifNotExists || !exists(executor)) {
            // Create first table using current schema
            val hiveTableRelation = tableRelation(1)
            hiveTableRelation.create(executor, false)

            // Create initial view
            val spark = executor.spark
            val df = spark.read.table(hiveTableRelation.tableIdentifier.unquotedString)
            val sql = new SQLBuilder(df).toSQL
            val hiveViewRelation = viewRelationFromSql(sql)
            hiveViewRelation.create(executor, false)
        }
    }

    /**
      * This will delete any physical representation of the relation. Depending on the type only some meta data like
      * a Hive table might be dropped or also the physical files might be deleted
      *
      * @param executor
      */
    override def destroy(executor: Executor, ifExists: Boolean): Unit = {
        require(executor != null)

        if (!ifExists || exists(executor)) {
            val catalog = executor.catalog

            // Destroy view
            val view = viewIdentifier
            logger.info(s"Dropping union view '$view' from Hive Union Table '$identifier'")
            catalog.dropView(view, false)

            // Destroy tables
            listTables(executor)
                .foreach { table =>
                    logger.info(s"Dropping backend table '$table' from Hive Union Table '$identifier'")
                    catalog.dropTable(table, false, true)
                }
        }
    }

    /**
      * This will update any existing relation to the specified metadata.
      *
      * @param executor
      */
    override def migrate(executor: Executor): Unit = {
        require(executor != null)

        val catalog = executor.catalog
        val sourceSchema = schema.get.sparkSchema
        val allTables = listTables(executor)

        // 1. Find all tables
        // 2. Find appropriate table
        val target = allTables
            .find { id =>
                val table = catalog.getTable(id)
                val targetSchema = table.dataSchema
                val targetFieldsByName = targetSchema.map(f => (f.name.toLowerCase(Locale.ROOT), f)).toMap

                sourceSchema.forall { field =>
                    targetFieldsByName.get(field.name.toLowerCase(Locale.ROOT))
                        .forall(tgt => SchemaUtils.isCompatible(field, tgt))
                }
            }

        val dirty = target.map { id =>
                // 3. If found:
                //  3.1 Migrate table (add new columns)
                val table = catalog.getTable(id)
                val targetSchema = table.dataSchema
                val targetFields = targetSchema.map(f => f.name.toLowerCase(Locale.ROOT)).toSet

                val missingFields = sourceSchema.filterNot(f => targetFields.contains(f.name.toLowerCase(Locale.ROOT)))
                if (missingFields.nonEmpty) {
                    logger.info(s"Migrating HiveUntionTable relation '$identifier' by adding new columns ${missingFields.map(_.name).mkString(",")} to Hive table '$id'")
                    catalog.addTableColumns(id, missingFields)
                    true
                }
                else {
                    false
                }
            }
            .getOrElse {
                // 3. If not found:
                //  3.2 Create new table
                val tableSet = allTables.toSet
                val version = (1 to 100000).find(n => !tableSet.contains(tableIdentifier(n))).get
                logger.info(s"Migrating HiveUntionTable relation '$identifier' by creating new Hive table '${tableIdentifier(version)}'")
                val hiveTableRelation = tableRelation(version)
                hiveTableRelation.create(executor, false)
                true
            }

        if (dirty) {
            //  4 Adjust union view
            val hiveViewRelation = viewRelationFromTables(executor)
            hiveViewRelation.migrate(executor)
        }
    }
}




class HiveUnionTableRelationSpec extends RelationSpec with SchemaRelationSpec with PartitionedRelationSpec {
    @JsonProperty(value = "tableDatabase", required = false) private var tableDatabase: Option[String] = None
    @JsonProperty(value = "tablePrefix", required = true) private var tablePrefix: String = ""
    @JsonProperty(value = "locationPrefix", required = false) private var locationPrefix: Option[String] = None
    @JsonProperty(value = "viewDatabase", required = false) private var viewDatabase: Option[String] = None
    @JsonProperty(value = "view", required = true) private var view: String = ""
    @JsonProperty(value = "external", required = false) private var external: String = "false"
    @JsonProperty(value = "format", required = false) private var format: String = _
    @JsonProperty(value = "rowFormat", required = false) private var rowFormat: Option[String] = None
    @JsonProperty(value = "inputFormat", required = false) private var inputFormat: Option[String] = None
    @JsonProperty(value = "outputFormat", required = false) private var outputFormat: Option[String] = None
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map()
    @JsonProperty(value = "serdeProperties", required = false) private var serdeProperties: Map[String, String] = Map()

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): HiveUnionTableRelation = {
        new HiveUnionTableRelation(
            instanceProperties(context),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            context.evaluate(tableDatabase),
            context.evaluate(tablePrefix),
            context.evaluate(locationPrefix).map(p => new Path(context.evaluate(p))),
            context.evaluate(viewDatabase),
            context.evaluate(view),
            context.evaluate(external).toBoolean,
            context.evaluate(format),
            context.evaluate(rowFormat),
            context.evaluate(inputFormat),
            context.evaluate(outputFormat),
            context.evaluate(properties),
            context.evaluate(serdeProperties)
        )
    }
}
