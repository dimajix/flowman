/*
 * Copyright 2022 Kaya Kupferschmidt
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

import java.io.StringWriter
import java.nio.charset.Charset

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.create.view.CreateView
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.util.TablesNamesFinder
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableType
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MigrationFailedException
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.jdbc.JdbcUtils
import com.dimajix.flowman.jdbc.SqlDialects
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.spec.connection.ConnectionReferenceSpec
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SchemaUtils
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StructType


case class JdbcViewRelation(
    override val instanceProperties:Relation.Properties,
    override val partitions: Seq[PartitionField] = Seq.empty,
    connection: Reference[Connection],
    view: TableIdentifier,
    properties: Map[String,String] = Map.empty,
    sql: Option[String] = None,
    file: Option[Path] = None
) extends JdbcRelation(
    connection,
    properties + (JDBCOptions.JDBC_TABLE_NAME -> view.unquotedString)
) with PartitionedRelation {
    protected val resource: ResourceIdentifier = ResourceIdentifier.ofJdbcTable(view)
    /**
     * Returns the list of all resources which will be created by this relation. This method mainly refers to the
     * CREATE and DESTROY execution phase.
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
                allPartitions.map(p => ResourceIdentifier.ofJdbcTablePartition(view, p.toMap)).toSet
            case Operation.WRITE =>
                dependencies.map(l => ResourceIdentifier.ofJdbcTablePartition(TableIdentifier(l), Map.empty)).toSet
        }
    }

    /**
     *
     * Returns the list of all resources which will be required by this relation for creation. This method mainly
     * refers to the CREATE and DESTROY execution phase.
     *
     * @return
     */
    override def requires(op:Operation, partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier] = {
        op match {
            case Operation.CREATE | Operation.DESTROY =>
                val db = view.database.map(db => ResourceIdentifier.ofJdbcDatabase(db)).toSet
                val tables = dependencies.map(l => ResourceIdentifier.ofJdbcTable(TableIdentifier(l))).toSet
                db ++ tables
            case Operation.READ =>
                val other = dependencies.map(l => ResourceIdentifier.ofJdbcTablePartition(TableIdentifier(l), Map.empty)).toSet
                other ++ Set(resource)
            case Operation.WRITE =>
                Set(resource)
        }
    }

    /**
     * Returns the schema of the relation, either from an explicitly specified schema or by schema inference from
     * the physical source
     * @param execution
     * @return
     */
    override def describe(execution:Execution, partitions:Map[String,FieldValue] = Map()) : StructType = {
        val result = withConnection { (con, options) =>
            JdbcUtils.getTableSchema(con, view, options)
        }

        applyDocumentation(result)
    }

    /**
     * Reads data from the relation, possibly from specific partitions
     *
     * @param execution
     * @param partitions - List of partitions. If none are specified, all the data will be read
     * @return
     */
    override def read(execution: Execution, partitions: Map[String, FieldValue]): DataFrame = {
        require(execution != null)
        require(partitions != null)

        logger.info(s"Reading JDBC view relation '$identifier' from table $view via connection '$connection' partition $partitions")

        // Get Connection
        val (_,props) = createConnectionProperties()

        // Read from database. We do not use this.reader, because Spark JDBC sources do not support explicit schemas
        val reader = execution.spark.read
            .format("jdbc")
            .options(props)

        val tableDf = reader
            .option(JDBCOptions.JDBC_TABLE_NAME, view.unquotedString)
            .load()

        val filteredDf = filterPartition(tableDf, partitions)

        // Apply embedded schema, if it is specified. This will remove/cast any columns not present in the
        // explicit schema specification of the relation
        applyInputSchema(execution, filteredDf)
    }

    /**
     * Writes data into the relation, possibly into a specific partition
     *
     * @param execution
     * @param df        - dataframe to write
     * @param partition - destination partition
     */
    override def write(execution: Execution, df: DataFrame, partition: Map[String, SingleValue], mode: OutputMode): Unit =  {
        throw new UnsupportedOperationException()
    }

    /**
     * Removes one or more partitions.
     *
     * @param execution
     * @param partitions
     */
    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
    }

    /**
     * Returns true if the relation already exists, otherwise it needs to be created prior usage. This refers to
     * the relation itself, not to the data or a specific partition. [[loaded]] should return [[Yes]] after
     * [[[create]] has been called, and it should return [[No]] after [[destroy]] has been called.
     *
     * @param execution
     * @return
     */
    override def exists(execution: Execution): Trilean = {
        require(execution != null)

        withConnection { (con, options) =>
            JdbcUtils.tableExists(con, view, options)
        }
    }

    /**
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     *
     * @param execution
     * @return
     */
    override def conforms(execution: Execution, migrationPolicy: MigrationPolicy): Trilean = {
        withConnection { (con, options) =>
            val dialect = SqlDialects.get(options.url)
            if (JdbcUtils.tableExists(con, view, options)) {
                val currentTable = JdbcUtils.getTableOrView(con, view, options)
                // Check if the current entity is a VIEW
                if (currentTable.tableType != TableType.VIEW && currentTable.tableType != TableType.UNKNOWN) {
                    No
                }
                else if (!dialect.supportsExactViewRetrieval) {
                    // When we cannot retrieve the original SQL text, we fall back to comparing the schema
                    val desiredSchema = JdbcUtils.getQuerySchema(con, statement, options)
                    val actualSchema = JdbcUtils.getTableSchema(con, view, options)
                    if (SchemaUtils.normalize(desiredSchema) != SchemaUtils.normalize(actualSchema))
                        No
                    else
                        Unknown
                }
                else {
                    // Compare view definition text
                    val desiredSql = statement
                    val currentSql = JdbcUtils.getViewDefinition(con, view, options)
                    normalizeViewSql(desiredSql) == normalizeViewSql(currentSql)
                }
            }
            else {
                No
            }
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
     * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
     * relation will not contain any data, but all metadata will be processed
     *
     * @param execution
     */
    override def create(execution: Execution, ifNotExists: Boolean): Unit = {
        withConnection{ (con,options) =>
            if (!ifNotExists || !JdbcUtils.tableExists(con, view, options)) {
                doCreate(con, options)
                execution.refreshResource(resource)
            }
        }
    }
    protected def doCreate(con:java.sql.Connection, options:JDBCOptions): Unit = {
        JdbcUtils.createView(con, view, statement, options)
    }

    /**
     * This will delete any physical representation of the relation. Depending on the type only some meta data like
     * a Hive table might be dropped or also the physical files might be deleted
     *
     * @param execution
     */
    override def destroy(execution: Execution, ifExists: Boolean): Unit = {
        dropTableOrView(execution, view, ifExists)
    }

    /**
     * This will update any existing relation to the specified metadata.
     *
     * @param execution
     */
    override def migrate(execution: Execution, migrationPolicy: MigrationPolicy, migrationStrategy: MigrationStrategy): Unit = {
        // Only try migration if schema is explicitly specified
        withConnection { (con, options) =>
            if (JdbcUtils.tableExists(con, view, options)) {
                val currentTable = JdbcUtils.getTableOrView(con, view, options)

                // Check if table type changes
                if (currentTable.tableType != TableType.UNKNOWN && currentTable.tableType != TableType.VIEW) {
                    // Drop view, recreate table
                    migrateFromTable(con, options, migrationStrategy)
                }
                else {
                    migrateFromView(con, options, migrationStrategy)
                }
            }
        }
    }

    private def migrateFromView(connection:java.sql.Connection, options:JDBCOptions, migrationStrategy:MigrationStrategy) : Unit = {
        withConnection { (con, options) =>
            val dialect = SqlDialects.get(options.url)
            val requiresMigration = if (!dialect.supportsExactViewRetrieval) {
                logger.warn(s"JDBC database of relation '$identifier' does not support retrieval of exact VIEW definitions. Therefore Flowman will migrate it anyway")
                true
            }
            else {
                // Compare view definition text
                val desiredSql = statement
                val currentSql = JdbcUtils.getViewDefinition(con, view, options)
                normalizeViewSql(desiredSql) != normalizeViewSql(currentSql)
            }

            if (requiresMigration) {
                migrationStrategy match {
                    case MigrationStrategy.NEVER =>
                        logger.warn(s"Migration required for JdbcView relation '$identifier' of VIEW $view, but migrations are disabled.")
                    case MigrationStrategy.FAIL =>
                        logger.error(s"Cannot migrate relation JdbcView '$identifier' of VIEW $view, since migrations are disabled.")
                        throw new MigrationFailedException(identifier)
                    case MigrationStrategy.ALTER|MigrationStrategy.ALTER_REPLACE|MigrationStrategy.REPLACE =>
                        logger.info(s"Migrating JdbcView relation '$identifier' with VIEW $view")
                        JdbcUtils.alterView(con, view, statement, options)
                }
            }
        }
    }

    private def migrateFromTable(connection:java.sql.Connection, options:JDBCOptions, migrationStrategy:MigrationStrategy) : Unit = {
        migrationStrategy match {
            case MigrationStrategy.NEVER =>
                logger.warn(s"Migration required for JdbcView relation '$identifier' from TABLE to a VIEW $view, but migrations are disabled.")
            case MigrationStrategy.FAIL =>
                logger.error(s"Cannot migrate JdbcView relation '$identifier' from TABLE to a VIEW $view, since migrations are disabled.")
                throw new MigrationFailedException(identifier)
            case MigrationStrategy.ALTER|MigrationStrategy.ALTER_REPLACE|MigrationStrategy.REPLACE =>
                logger.info(s"Migrating JdbcView relation '$identifier' from TABLE to VIEW $view")
                try {
                    withStatement(connection, options) { (stmt,options) =>
                        JdbcUtils.dropTable(stmt, view, options)
                    }
                    doCreate(connection, options)
                }
                catch {
                    case NonFatal(ex) => throw new MigrationFailedException(identifier, ex)
                }
        }
    }

    private lazy val statement : String = {
        sql
            .orElse(file.map { f =>
                val fs = context.fs
                val input = fs.file(f).open()
                try {
                    val writer = new StringWriter()
                    IOUtils.copy(input, writer, Charset.forName("UTF-8"))
                    writer.toString
                }
                finally {
                    input.close()
                }
            })
            .getOrElse(
                throw new IllegalArgumentException("JdbcView either requires explicit SQL SELECT statement or file")
            )
    }

    private lazy val dependencies : Seq[String] = {
        val parsed = CCJSqlParserUtil.parse(statement)
        val tablesNamesFinder = new TablesNamesFinder()
        tablesNamesFinder.getTableList(parsed).asScala
    }

    private def normalizeViewSql(sql:String) : String = {
        CCJSqlParserUtil.parse(sql) match {
            case s:Select => s.toString
            case c:CreateView => c.getSelect.toString
        }
    }
}



class JdbcViewRelationSpec extends RelationSpec with PartitionedRelationSpec{
    @JsonProperty(value="connection", required = true) private var connection: ConnectionReferenceSpec = _
    @JsonProperty(value="properties", required = false) private var properties: Map[String, String] = Map.empty
    @JsonProperty(value="database", required = false) private var database: Option[String] = None
    @JsonPropertyDescription("Name of the JDBC view")
    @JsonProperty(value="view", required = true) private var view: String = _
    @JsonPropertyDescription("SQL query for the view definition. This has to be specified in database specific SQL syntax.")
    @JsonProperty(value="sql", required = false) private var sql: Option[String] = None
    @JsonPropertyDescription("Name of a file containing the SQL query for the view definition. This has to be specified in database specific SQL syntax.")
    @JsonProperty(value="file", required=false) private var file:Option[String] = None

    /**
     * Creates the instance of the specified Relation with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context, props:Option[Relation.Properties] = None): JdbcViewRelation = {
        JdbcViewRelation(
            instanceProperties(context, props),
            partitions.map(_.instantiate(context)),
            connection.instantiate(context),
            TableIdentifier(context.evaluate(view), context.evaluate(database)),
            context.evaluate(properties),
            context.evaluate(sql),
            context.evaluate(file).map(p => new Path(p))
        )
    }
}
