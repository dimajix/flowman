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

import java.sql.Connection
import java.sql.Statement
import java.util.Properties

import scala.collection.JavaConverters._

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.PartitionAlreadyExistsException
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.common.Trilean
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.jdbc.JdbcUtils
import com.dimajix.flowman.jdbc.SqlDialect
import com.dimajix.flowman.jdbc.SqlDialects
import com.dimajix.flowman.jdbc.TableDefinition
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


case class JdbcRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema],
    override val partitions: Seq[PartitionField],
    connection: ConnectionIdentifier,
    properties: Map[String,String] = Map(),
    database: Option[String] = None,
    table: Option[String] = None,
    query: Option[String] = None
) extends BaseRelation with PartitionedRelation with SchemaRelation {
    private val logger = LoggerFactory.getLogger(classOf[JdbcRelation])
    private lazy val jdbcConnection = context.getConnection(connection).asInstanceOf[JdbcConnection]

    def tableIdentifier : TableIdentifier = TableIdentifier(table.getOrElse(""), database)


    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides: Set[ResourceIdentifier] = {
        table.map(t => ResourceIdentifier.ofJdbcTable(t, database)).toSet
    }

    /**
      * Returns the list of all resources which will be required by this relation for creation.
      *
      * @return
      */
    override def requires: Set[ResourceIdentifier] = {
        database.map(db => ResourceIdentifier.ofJdbcDatabase(db)).toSet
    }

    /**
      * Returns the list of all resources which will are managed by this relation for reading or writing a specific
      * partition. The list will be specifically  created for a specific partition, or for the full relation (when the
      * partition is empty)
      *
      * @param partitions
      * @return
      */
    override def resources(partitions: Map[String, FieldValue]): Set[ResourceIdentifier] = {
        require(partitions != null)

        requireValidPartitionKeys(partitions)

        val allPartitions = PartitionSchema(this.partitions).interpolate(partitions)
        allPartitions.map(p => ResourceIdentifier.ofJdbcTablePartition(table.getOrElse(""), database, p.toMap)).toSet
    }

    /**
      * Reads the configured table from the source
      * @param executor
      * @param schema
      * @return
      */
    override def read(executor:Executor, schema:Option[StructType], partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(executor != null)
        require(schema != null)
        require(partitions != null)

        // Get Connection
        val (url,props) = createProperties()

        // Read from database. We do not use this.reader, because Spark JDBC sources do not support explicit schemas
        val reader = executor.spark.read.options(options)

        val tableDf =
            if (query.nonEmpty) {
                logger.info(s"Reading data from JDBC source '$identifier' using connection '$connection' using partition values $partitions")
                reader.format("jdbc")
                    .option("query", query.get)
                    .option("url", url)
                    .options(props.asScala)
                    .load()
            }
            else {
                logger.info(s"Reading data from JDBC table $tableIdentifier using connection '$connection' using partition values $partitions")
                reader.jdbc(url, tableIdentifier.unquotedString, props)
            }

        val df = filterPartition(tableDf, partitions)
        SchemaUtils.applySchema(df, schema)
    }

    /**
      * Writes a given DataFrame into a JDBC connection
      *
      * @param executor
      * @param df
      * @param partition
      * @param mode
      */
    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:OutputMode) : Unit = {
        require(executor != null)
        require(df != null)
        require(partition != null)

        if (query.nonEmpty)
            throw new UnsupportedOperationException(s"Cannot write into JDBC relation '$identifier' which is defined by an SQL query")

        logger.info(s"Writing data to JDBC relation '$identifier' for table $tableIdentifier using connection '$connection' with mode '$mode'")

        // Get Connection
        val (url,props) = createProperties()
        val dialect = SqlDialects.get(url)

        // Write partition into DataBase
        val dfExt = addPartition(df, partition)

        if (partition.isEmpty) {
            // Write partition into DataBase
            this.writer(executor, dfExt)
                .mode(mode.batchMode)
                .jdbc(url, tableIdentifier.unquotedString, props)
        }
        else {
            def writePartition(): Unit = {
                this.writer(executor, dfExt)
                    .mode(SaveMode.Append)
                    .jdbc(url, tableIdentifier.unquotedString, props)
            }

            mode match {
                case OutputMode.OVERWRITE =>
                    withStatement { (statement, _) =>
                        val condition = partitionCondition(dialect, partition)
                        val query = "DELETE FROM " + dialect.quote(tableIdentifier) + " WHERE " + condition
                        statement.executeUpdate(query)
                    }
                    writePartition()
                case OutputMode.APPEND =>
                    writePartition()
                case OutputMode.IGNORE_IF_EXISTS =>
                    if (!checkPartition(partition)) {
                        writePartition()
                    }
                case OutputMode.ERROR_IF_EXISTS =>
                    if (!checkPartition(partition)) {
                        writePartition()
                    }
                    else {
                        throw new PartitionAlreadyExistsException(database.getOrElse(""), table.get, partition.mapValues(_.value))
                    }
                case _ => throw new IllegalArgumentException(s"Unknown save mode: '$mode'. " +
                    "Accepted save modes are 'overwrite', 'append', 'ignore', 'error', 'errorifexists'.")
            }
        }
    }

    /**
      * Removes one or more partitions.
      * @param executor
      * @param partitions
      */
    override def truncate(executor: Executor, partitions: Map[String, FieldValue]): Unit = {
        require(executor != null)
        require(partitions != null)

        if (query.nonEmpty)
            throw new UnsupportedOperationException(s"Cannot clean JDBC relation '$identifier' which is defined by an SQL query")

        if (partitions.isEmpty) {
            logger.info(s"Cleaning JDBC relation '$identifier', this will truncate JDBC table $tableIdentifier")
            withConnection { (con, options) =>
                JdbcUtils.truncateTable(con, tableIdentifier, options)
            }
        }
        else {
            logger.info(s"Cleaning partitions of JDBC relation '$identifier', this will partially truncate JDBC table $tableIdentifier")
            withStatement { (statement, options) =>
                val dialect = SqlDialects.get(options.url)
                val condition = partitionCondition(dialect, partitions)
                val query = "DELETE FROM " + dialect.quote(tableIdentifier) + " WHERE " + condition
                statement.executeUpdate(query)
            }
        }
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      * @param executor
      * @return
      */
    override def exists(executor:Executor) : Trilean = {
        require(executor != null)

        withConnection{ (con,options) =>
            JdbcUtils.tableExists(con, tableIdentifier, options)
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
    override def loaded(executor: Executor, partition: Map[String, SingleValue]): Trilean = {
        require(executor != null)
        require(partition != null)

        withConnection{ (con,options) =>
            val dialect = SqlDialects.get(options.url)
            val condition = partitionCondition(dialect, partition)

            JdbcUtils.tableExists(con, tableIdentifier, options) &&
                !JdbcUtils.emptyResult(con, tableIdentifier, condition, options)
        }
    }

    /**
      * This method will physically create the corresponding relation in the target JDBC database.
     *
     * @param executor
      */
    override def create(executor:Executor, ifNotExists:Boolean=false) : Unit = {
        require(executor != null)

        if (query.nonEmpty)
            throw new UnsupportedOperationException(s"Cannot create JDBC relation '$identifier' which is defined by an SQL query")

        logger.info(s"Creating JDBC relation '$identifier', this will create JDBC table $tableIdentifier")
        withConnection{ (con,options) =>
            if (!ifNotExists || !JdbcUtils.tableExists(con, tableIdentifier, options)) {
                if (this.schema.isEmpty)
                    throw new UnsupportedOperationException(s"Cannot create JDBC relation '$identifier' without a schema")
                val schema = this.schema.get
                val table = TableDefinition(
                    tableIdentifier,
                    schema.fields ++ partitions.map(_.field),
                    schema.description,
                    schema.primaryKey
                )
                JdbcUtils.createTable(con, table, options)
            }
        }
    }

    /**
      * This method will physically destroy the corresponding relation in the target JDBC database.
      * @param executor
      */
    override def destroy(executor:Executor, ifExists:Boolean=false) : Unit = {
        require(executor != null)

        if (query.nonEmpty)
            throw new UnsupportedOperationException(s"Cannot destroy JDBC relation '$identifier' which is defined by an SQL query")

        logger.info(s"Destroying JDBC relation '$identifier', this will drop JDBC table $tableIdentifier")
        withConnection{ (con,options) =>
            if (!ifExists || JdbcUtils.tableExists(con, tableIdentifier, options)) {
                JdbcUtils.dropTable(con, tableIdentifier, options)
            }
        }
    }

    override def migrate(executor:Executor) : Unit = ???

    /**
      * Creates a Spark schema from the list of fields.
      * @return
      */
    override protected def inputSchema : Option[StructType] = {
        schema.map(s => StructType(s.fields.map(_.sparkField) ++ partitions.map(_.sparkField)))
    }

    /**
      * Creates a Spark schema from the list of fields. The list is used for output operations, i.e. for writing
      * @return
      */
    override protected def outputSchema : Option[StructType] = {
        schema.map(s => StructType(s.fields.map(_.sparkField) ++ partitions.map(_.sparkField)))
    }

    private def createProperties() = {
        val connection = jdbcConnection
        val props = new Properties()
        props.setProperty("driver", connection.driver)
        connection.username.foreach(props.setProperty("user", _))
        connection.password.foreach(props.setProperty("password", _))

        connection.properties.foreach(kv => props.setProperty(kv._1, kv._2))
        properties.foreach(kv => props.setProperty(kv._1, kv._2))

        (jdbcConnection.url,props)
    }

    private def withConnection[T](fn:(Connection,JDBCOptions) => T) : T = {
        val connection = jdbcConnection
        val props = scala.collection.mutable.Map[String,String]()
        props.update("driver", connection.driver)
        connection.username.foreach(props.update("user", _))
        connection.password.foreach(props.update("password", _))

        logger.info("Connecting to jdbc source at {}", connection.url)

        val options = new JDBCOptions(connection.url, tableIdentifier.unquotedString, props.toMap ++ connection.properties ++ properties)
        val conn = JdbcUtils.createConnection(options)
        try {
            fn(conn, options)
        }
        finally {
            conn.close()
        }
    }

    private def withStatement[T](fn:(Statement,JDBCOptions) => T) : T = {
        withConnection { (con, options) =>
            val statement = con.createStatement()
            try {
                statement.setQueryTimeout(JdbcUtils.queryTimeout(options))
                fn(statement, options)
            }
            finally {
                statement.close()
            }
        }
    }

    private def checkPartition(partition:Map[String,SingleValue]) : Boolean = {
        withConnection{ (connection, options) =>
            val dialect = SqlDialects.get(options.url)
            val condition = partitionCondition(dialect, partition)
            !JdbcUtils.emptyResult(connection, tableIdentifier, condition, options)
        }
    }

    private def partitionCondition(dialect:SqlDialect, partitions: Map[String, FieldValue]) : String = {
        val partitionSchema = PartitionSchema(this.partitions)
        partitions.map { case (name, value) =>
            val field = partitionSchema.get(name)
            dialect.expr.in(field.name, field.interpolate(value))
        }
        .mkString(" AND ")
    }
}




class JdbcRelationSpec extends RelationSpec with PartitionedRelationSpec with SchemaRelationSpec {
    @JsonProperty(value = "connection", required = true) private var connection: String = _
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map()
    @JsonProperty(value = "database", required = false) private var database: Option[String] = None
    @JsonProperty(value = "table", required = false) private var table: Option[String] = None
    @JsonProperty(value = "query", required = false) private var query: Option[String] = None

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): JdbcRelation = {
        JdbcRelation(
            instanceProperties(context),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            ConnectionIdentifier.parse(context.evaluate(connection)),
            context.evaluate(properties),
            database.map(context.evaluate).filter(_.nonEmpty),
            table.map(context.evaluate).filter(_.nonEmpty),
            query.map(context.evaluate).filter(_.nonEmpty)
        )
    }
}
