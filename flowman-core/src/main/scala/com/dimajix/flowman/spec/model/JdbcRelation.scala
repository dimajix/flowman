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

import java.sql.Connection
import java.sql.Statement
import java.util.Locale
import java.util.Properties

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.PartitionAlreadyExistsException
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.jdbc.JdbcUtils
import com.dimajix.flowman.jdbc.SqlDialect
import com.dimajix.flowman.jdbc.SqlDialects
import com.dimajix.flowman.jdbc.TableDefinition
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.spec.schema.PartitionField
import com.dimajix.flowman.spec.schema.PartitionSchema
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


case class JdbcRelation(
    instanceProperties:Relation.Properties,
    override val schema:Schema,
    override val partitions: Seq[PartitionField],
    connection: JdbcConnection,
    properties: Map[String,String],
    database: String,
    table: String
) extends BaseRelation with PartitionedRelation with SchemaRelation {
    private val logger = LoggerFactory.getLogger(classOf[JdbcRelation])

    def fqTable : String = Option(database).filter(_.nonEmpty).map(_ + ".").getOrElse("") + table
    def tableIdentifier : TableIdentifier = TableIdentifier(table, Option(database))

    /**
      * Reads the configured table from the source
      * @param executor
      * @param schema
      * @return
      */
    override def read(executor:Executor, schema:StructType, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        logger.info(s"Reading data from JDBC source '$tableIdentifier' using connection '${connection.identifier}' using partition values $partitions")

        // Get Connection
        val (url,props) = createProperties()

        // Read from database. We do not use this.reader, because Spark JDBC sources do not support explicit schemas
        val reader = executor.spark.read.options(options)
        val tableDf = reader.jdbc(url, tableIdentifier.unquotedString, props)
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
    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:String) : Unit = {
        logger.info(s"Writing data to JDBC source $tableIdentifier in database ${connection.identifier}")

        // Get Connection
        val (url,props) = createProperties()
        val dialect = SqlDialects.get(url)

        // Write partition into DataBase
        val dfExt = addPartition(df, partition)

        if (partition.isEmpty) {
            // Write partition into DataBase
            this.writer(executor, dfExt)
                .mode(mode)
                .jdbc(url, tableIdentifier.unquotedString, props)
        }
        else {
            def writePartition(): Unit = {
                this.writer(executor, dfExt)
                    .mode(SaveMode.Append)
                    .jdbc(url, tableIdentifier.unquotedString, props)
            }

            mode.toLowerCase(Locale.ROOT) match {
                case "overwrite" =>
                    withStatement { (statement, _) =>
                        val condition = partitionCondition(dialect, partition)
                        val query = "DELETE FROM " + dialect.quote(tableIdentifier) + " WHERE " + condition
                        statement.executeUpdate(query)
                    }
                    writePartition()
                case "append" =>
                    writePartition()
                case "ignore" =>
                    if (!checkPartition(partition)) {
                        writePartition()
                    }
                case "error" | "errorifexists" | "default" =>
                    if (!checkPartition(partition)) {
                        writePartition()
                    }
                    else {
                        throw new PartitionAlreadyExistsException(database, table, partition.mapValues(_.value))
                    }
                case _ => throw new IllegalArgumentException(s"Unknown save mode: $mode. " +
                    "Accepted save modes are 'overwrite', 'append', 'ignore', 'error', 'errorifexists'.")
            }
        }
    }

    /**
      * Removes one or more partitions.
      * @param executor
      * @param partitions
      */
    override def clean(executor: Executor, partitions: Map[String, FieldValue]): Unit = {
        if (partitions.isEmpty) {
            logger.info(s"Cleaning jdbc relation $name, this will clean jdbc table $tableIdentifier")
            withConnection { (con, options) =>
                JdbcUtils.truncateTable(con, tableIdentifier, options)
            }
        }
        else {
            logger.info(s"Cleaning partitions of jdbc relation $name, this will clean jdbc table $tableIdentifier")
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
    override def exists(executor:Executor) : Boolean = {
        require(executor != null)

        withConnection{ (con,options) =>
            JdbcUtils.tableExists(con, tableIdentifier, options)
        }
    }

    /**
      * This method will physically create the corresponding relation in the target JDBC database.
      * @param executor
      */
    override def create(executor:Executor, ifNotExists:Boolean=false) : Unit = {
        require(executor != null)

        logger.info(s"Creating jdbc relation $name, this will create jdbc table $tableIdentifier")
        withConnection{ (con,options) =>
            if (!ifNotExists || !JdbcUtils.tableExists(con, tableIdentifier, options)) {
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

        logger.info(s"Destroying jdbc relation $name, this will drop jdbc table $tableIdentifier")
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
    override protected def inputSchema : StructType = {
        if (schema != null) {
            StructType(schema.fields.map(_.sparkField) ++ partitions.map(_.sparkField))
        }
        else {
            null
        }
    }

    /**
      * Creates a Spark schema from the list of fields. The list is used for output operations, i.e. for writing
      * @return
      */
    override protected def outputSchema : StructType = {
        if (schema != null) {
            StructType(schema.fields.map(_.sparkField) ++ partitions.map(_.sparkField))
        }
        else {
            null
        }
    }

    private def createProperties() = {
        // Get Connection
        val props = new Properties()
        Option(connection.username).foreach(props.setProperty("user", _))
        Option(connection.password).foreach(props.setProperty("password", _))
        props.setProperty("driver", connection.driver)

        connection.properties.foreach(kv => props.setProperty(kv._1, kv._2))
        properties.foreach(kv => props.setProperty(kv._1, kv._2))

        logger.info("Connecting to jdbc source at {}", connection.url)

        (connection.url,props)
    }

    private def withConnection[T](fn:(Connection,JDBCOptions) => T) : T = {
        val props = scala.collection.mutable.Map[String,String]()
        Option(connection.username).foreach(props.update("user", _))
        Option(connection.password).foreach(props.update("password", _))
        props.update("driver", connection.driver)

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
    @JsonProperty(value = "connection", required = true) private var _connection: String = _
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map()
    @JsonProperty(value = "database", required = true) private var database: String = _
    @JsonProperty(value = "table", required = true) private var table: String = _

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): JdbcRelation = {
        JdbcRelation(
            instanceProperties(context),
            if (schema != null) schema.instantiate(context) else null,
            partitions.map(_.instantiate(context)),
            context.getConnection(ConnectionIdentifier.parse(context.evaluate(_connection))).asInstanceOf[JdbcConnection],
            context.evaluate(properties),
            context.evaluate(database),
            context.evaluate(table)
        )
    }
}
