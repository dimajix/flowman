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

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.ExecutionException
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.jdbc.HiveDialect
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.transforms.SchemaEnforcer
import com.dimajix.flowman.transforms.UnionTransformer
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils
import com.dimajix.spark.sql.catalyst.SqlBuilder


object HiveUnionTableRelation {
    /**
     * Method for generating the UNION SQL. It is moved into the object to be able to unittest it easily
     * @param tables
     * @param schema
     * @return
     */
    private[relation] def unionSql(tables:Seq[DataFrame], schema:StructType) : String = {
        val union = UnionTransformer().transformDataFrames(tables)
        val conformed = SchemaEnforcer(schema).transform(union)
        new SqlBuilder(conformed).toSQL
    }
}

case class HiveUnionTableRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq(),
    tableDatabase: Option[String] = None,
    tablePrefix: String,
    locationPrefix: Option[Path] = None,
    viewDatabase: Option[String] = None,
    view: String,
    external: Boolean = false,
    format: String = "parquet",
    rowFormat: Option[String] = None,
    inputFormat: Option[String] = None,
    outputFormat: Option[String] = None,
    properties: Map[String, String] = Map(),
    serdeProperties: Map[String, String] = Map()
)  extends BaseRelation with PartitionedRelation {
    private val logger = LoggerFactory.getLogger(classOf[HiveUnionTableRelation])

    def viewIdentifier: TableIdentifier = TableIdentifier(view, viewDatabase)
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
        HiveViewRelation(
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
        val finalSchema = schema.get.sparkSchema.fields ++ partitions.map(_.sparkField)
        val sql = HiveUnionTableRelation.unionSql(df, StructType(finalSchema))
        viewRelationFromSql(sql)
    }

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides: Set[ResourceIdentifier] = Set(
        ResourceIdentifier.ofHiveTable(tablePrefix + "_[0-9]+", tableDatabase),
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
        allPartitions.map(p => ResourceIdentifier.ofHivePartition(tablePrefix + "_[0-9]+", tableDatabase, p.toMap)).toSet
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

        logger.info(s"Reading from Hive union relation '$identifier' from UNION VIEW $viewIdentifier using partition values $partitions")

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
    override def write(executor: Executor, df: DataFrame, partition: Map[String, SingleValue], mode: OutputMode): Unit = {
        require(executor != null)

        requireAllPartitionKeys(partition)

        val catalog = executor.catalog
        val partitionSchema = PartitionSchema(this.partitions)
        val partitionSpec = partitionSchema.spec(partition)
        logger.info(s"Writing to Hive union relation '$identifier' using partition values ${HiveDialect.expr.partition(partitionSpec)}")

        // 1. Find all tables
        val allTables = listTables(executor)

        // 2. Find appropriate table
        val table = allTables.find { id =>
                val table = catalog.getTable(id)
                SchemaUtils.isCompatible(df.schema, table.schema)
            }
            .getOrElse {
                logger.error(s"Cannot find appropriate target table for Hive Union Table '$identifier'. Required schema is\n ${df.schema.treeString}")
                throw new ExecutionException(s"Cannot find appropriate target table for Hive Union Table '$identifier'")
            }

        // 3. Drop  partition from all other tables
        allTables.filter(_ != table).foreach { table =>
            catalog.dropPartition(table, partitionSpec, ignoreIfNotExists=true)
        }

        // 4. Write to that table
        val relation = tableRelation(table, None)
        relation.write(executor, df, partition, OutputMode.OVERWRITE)
    }

    /**
      * Removes one or more partitions.
      *
      * @param executor
      * @param partitions
      */
    override def truncate(executor: Executor, partitions: Map[String, FieldValue]): Unit = {
        require(executor != null)
        require(partitions != null)

        logger.info(s"Truncating Hive union relation '$identifier' partition $partitions")

        listTables(executor)
            .foreach { table =>
                val relation = tableRelation(table, None)
                relation.truncate(executor, partitions)
            }
    }


    /**
     * Returns true if the target partition exists and contains valid data. Absence of a partition indicates that a
     * [[write]] is required for getting up-to-date contents. A [[write]] with output mode
     * [[OutputMode.ERROR_IF_EXISTS]] then should not throw an error but create the corresponding partition
     *
     * @param executor
     * @param partition
     * @return
     */
    override def exists(executor: Executor, partition: Map[String, SingleValue]): Trilean = {
        require(executor != null)
        require(partition != null)

        requireAllPartitionKeys(partition)

        val catalog = executor.catalog

        if (partition.isEmpty) {
            if (catalog.tableExists(viewIdentifier))
                Unknown
            else
                No
        }
        else {
            val partitionSchema = PartitionSchema(this.partitions)
            val partitionSpec = partitionSchema.spec(partition)

            catalog.tableExists(viewIdentifier) &&
                listTables(executor).exists { table =>
                    catalog.partitionExists(table, partitionSpec)
                }
        }
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      *
      * @param executor
      * @return
      */
    override def exists(executor: Executor): Trilean = {
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

        if (!ifNotExists || exists(executor) == No) {
            logger.info(s"Creating Hive union relation '$identifier'")
            // Create first table using current schema
            val hiveTableRelation = tableRelation(1)
            hiveTableRelation.create(executor, ifNotExists)

            // Create initial view
            val spark = executor.spark
            val df = spark.read.table(hiveTableRelation.tableIdentifier.unquotedString)
            val sql = new SqlBuilder(df).toSQL
            val hiveViewRelation = viewRelationFromSql(sql)
            hiveViewRelation.create(executor, ifNotExists)
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

        if (!ifExists || exists(executor) == Yes) {
            val catalog = executor.catalog

            // Destroy view
            logger.info(s"Dropping Hive union relation '$identifier' UNION VIEW $viewIdentifier")
            catalog.dropView(viewIdentifier, ifExists)

            // Destroy tables
            listTables(executor)
                .foreach { table =>
                    logger.info(s"Dropping Hive union relation '$identifier' backend table '$table'")
                    catalog.dropTable(table, false)
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

        target match {
            case Some(id) =>
                // 3. If found:
                //  3.1 Migrate table (add new columns)
                val table = catalog.getTable(id)
                val targetSchema = table.dataSchema
                val targetFields = targetSchema.map(f => f.name.toLowerCase(Locale.ROOT)).toSet

                val missingFields = sourceSchema.filterNot(f => targetFields.contains(f.name.toLowerCase(Locale.ROOT)))
                if (missingFields.nonEmpty) {
                    val newSchema = StructType(targetSchema.fields ++ missingFields)
                    logger.info(s"Migrating Hive Untion Table relation '$identifier' by adding new columns ${missingFields.map(_.name).mkString(",")} to Hive table $id. New schema is\n ${newSchema.treeString}")
                    catalog.addTableColumns(id, missingFields)
                }

            case None =>
                // 3. If not found:
                //  3.2 Create new table
                val tableSet = allTables.toSet
                val version = (1 to 100000).find(n => !tableSet.contains(tableIdentifier(n))).get
                logger.info(s"Migrating Hive Untion Table relation '$identifier' by creating new Hive table ${tableIdentifier(version)}")
                val hiveTableRelation = tableRelation(version)
                hiveTableRelation.create(executor, false)
        }

        //  4 Always migrate union view, maybe SQL generator changed
        val hiveViewRelation = viewRelationFromTables(executor)
        hiveViewRelation.migrate(executor)
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
        HiveUnionTableRelation(
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
