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

import java.sql.Statement
import java.util.Locale

import scala.collection.mutable
import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.dimajix.common.SetIgnoreCase
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MergeClause
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.jdbc.JdbcUtils
import com.dimajix.flowman.jdbc.SqlDialects
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.spec.connection.ConnectionReferenceSpec
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.{StructType => FlowmanStructType}


case class JdbcQueryRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq.empty,
    connection: Reference[Connection],
    query: String,
    properties: Map[String,String] = Map.empty
) extends BaseRelation with PartitionedRelation with SchemaRelation {
    protected val logger: Logger = LoggerFactory.getLogger(getClass)


    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides: Set[ResourceIdentifier] = Set.empty

    /**
      * Returns the list of all resources which will be required by this relation for creation.
      *
      * @return
      */
    override def requires: Set[ResourceIdentifier] = Set.empty

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

        Set(ResourceIdentifier.ofJdbcQuery(query))
    }

    /**
     * Returns the schema of the relation, either from an explicitly specified schema or by schema inference from
     * the physical source
     * @param execution
     * @return
     */
    override def describe(execution:Execution, partitions:Map[String,FieldValue] = Map()) : FlowmanStructType = {
        val result = if (schema.nonEmpty) {
            FlowmanStructType(fields)
        }
        else {
            withConnection { (con, options) =>
                JdbcUtils.getQuerySchema(con, query, options)
            }
        }

        applyDocumentation(result)
    }

    /**
      * Reads the configured table from the source
      * @param execution
      * @return
      */
    override def read(execution:Execution, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(execution != null)
        require(partitions != null)

        // Get Connection
        val (_,props) = createConnectionProperties()

        // Read from database. We do not use this.reader, because Spark JDBC sources do not support explicit schemas
        val reader = execution.spark.read
            .format("jdbc")
            .options(props)

        logger.info(s"Reading JDBC relation '$identifier' with a custom query via connection '$connection' partition $partitions")
        val tableDf = reader
            .option(JDBCOptions.JDBC_QUERY_STRING, query)
            .load()

        val filteredDf = filterPartition(tableDf, partitions)

        // Apply embedded schema, if it is specified. This will remove/cast any columns not present in the
        // explicit schema specification of the relation
        //SchemaUtils.applySchema(filteredDf, inputSchema, insertNulls=false)
        applyInputSchema(execution, filteredDf, includePartitions = false)
    }

    /**
      * Writes a given DataFrame into a JDBC connection
      *
      * @param execution
      * @param df
      * @param partition
      * @param mode
      */
    override def write(execution:Execution, df:DataFrame, partition:Map[String,SingleValue], mode:OutputMode) : Unit = {
        require(execution != null)
        require(df != null)
        require(partition != null)

        throw new UnsupportedOperationException(s"Cannot write into JDBC query relation '$identifier' which is defined by an SQL query")
    }

    /**
     * Performs a merge operation. Either you need to specify a [[mergeKey]], or the relation needs to provide some
     * default key.
     *
     * @param execution
     * @param df
     * @param mergeCondition
     * @param clauses
     */
    override def merge(execution: Execution, df: DataFrame, condition:Option[Column], clauses: Seq[MergeClause]): Unit = {
        throw new UnsupportedOperationException(s"Cannot write into JDBC relation '$identifier' which is defined by an SQL query")
    }

    /**
      * Removes one or more partitions.
     *
     * @param execution
      * @param partitions
      */
    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
        require(execution != null)
        require(partitions != null)

        throw new UnsupportedOperationException(s"Cannot clean JDBC relation '$identifier' which is defined by an SQL query")
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      * @param execution
      * @return
      */
    override def exists(execution:Execution) : Trilean = {
        require(execution != null)
        true
    }

    /**
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     *
     * @param execution
     * @return
     */
    override def conforms(execution: Execution, migrationPolicy: MigrationPolicy): Trilean = {
        true
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
        require(execution != null)
        require(partition != null)

        Yes
    }

    /**
      * This method will physically create the corresponding relation in the target JDBC database.
     *
     * @param execution
      */
    override def create(execution:Execution, ifNotExists:Boolean=false) : Unit = {
        require(execution != null)

        throw new UnsupportedOperationException(s"Cannot create JDBC relation '$identifier' which is defined by an SQL query")
    }

    /**
      * This method will physically destroy the corresponding relation in the target JDBC database.
      * @param execution
      */
    override def destroy(execution:Execution, ifExists:Boolean=false) : Unit = {
        require(execution != null)

        throw new UnsupportedOperationException(s"Cannot destroy JDBC relation '$identifier' which is defined by an SQL query")
    }

    override def migrate(execution:Execution, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy) : Unit = {
        throw new UnsupportedOperationException(s"Cannot migrate JDBC relation '$identifier' which is defined by an SQL query")
    }

    /**
      * Creates a Spark schema from the list of fields. This JDBC implementation will add partition columns, since
      * these will be present while reading.
      * @return
      */
    override protected def inputSchema : Option[StructType] = {
        schema.map { s =>
            val partitions = this.partitions
            val partitionFields = SetIgnoreCase(partitions.map(_.name))
            StructType(s.fields.map(_.sparkField).filter(f => !partitionFields.contains(f.name)) ++ partitions.map(_.sparkField))
        }
    }

    protected def createConnectionProperties() : (String,Map[String,String]) = {
        val connection = this.connection.value.asInstanceOf[JdbcConnection]
        val props = mutable.Map[String,String]()
        props.put(JDBCOptions.JDBC_URL, connection.url)
        props.put(JDBCOptions.JDBC_DRIVER_CLASS, connection.driver)
        connection.username.foreach(props.put("user", _))
        connection.password.foreach(props.put("password", _))

        connection.properties.foreach(kv => props.put(kv._1, kv._2))
        properties.foreach(kv => props.put(kv._1, kv._2))

        (connection.url,props.toMap)
    }

    protected def withConnection[T](fn:(java.sql.Connection,JDBCOptions) => T) : T = {
        val (url,props) = createConnectionProperties()
        logger.debug(s"Connecting to jdbc source at $url")

        val options = new JDBCOptions(url, "", props)
        val conn = try {
            JdbcUtils.createConnection(options)
        } catch {
            case NonFatal(e) =>
                logger.error(s"Error connecting to jdbc source at $url: ${e.getMessage}")
                throw e
        }

        try {
            fn(conn, options)
        }
        finally {
            conn.close()
        }
    }

    protected def withTransaction[T](con:java.sql.Connection)(fn: => T) : T = {
        JdbcUtils.withTransaction(con)(fn)
    }

    protected def withStatement[T](fn:(Statement,JDBCOptions) => T) : T = {
        withConnection { (con, options) =>
            withStatement(con,options)(fn)
        }
    }

    protected def withStatement[T](con:java.sql.Connection,options:JDBCOptions)(fn:(Statement,JDBCOptions) => T) : T = {
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



class JdbcQueryRelationSpec extends RelationSpec with PartitionedRelationSpec with SchemaRelationSpec {
    @JsonProperty(value = "connection", required = true) private var connection: ConnectionReferenceSpec = _
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map.empty
    @JsonProperty(value = "query", required = true) private var query: String = ""

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, props:Option[Relation.Properties] = None): JdbcQueryRelation = {
        JdbcQueryRelation(
            instanceProperties(context, props),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            connection.instantiate(context),
            context.evaluate(query),
            context.evaluate(properties)
        )
    }
}
