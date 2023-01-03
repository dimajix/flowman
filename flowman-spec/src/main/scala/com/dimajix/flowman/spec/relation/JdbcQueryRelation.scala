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

import scala.collection.JavaConverters._

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.util.TablesNamesFinder
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MergeClause
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.fs.FileUtils
import com.dimajix.flowman.jdbc.JdbcUtils
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.spec.connection.ConnectionReferenceSpec
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.{StructType => FlowmanStructType}


case class JdbcQueryRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq.empty,
    connection: Reference[Connection],
    sql: Option[String] = None,
    file: Option[File] = None,
    properties: Map[String,String] = Map.empty
) extends JdbcRelation(
    connection,
    properties
) with SchemaRelation {
    protected val resource: ResourceIdentifier = ResourceIdentifier.ofJdbcQuery(query)

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides(op:Operation, partitions:Map[String,FieldValue] = Map.empty): Set[ResourceIdentifier] = {
        op match {
            case Operation.CREATE | Operation.DESTROY =>
                Set.empty
            case Operation.READ =>
                requireValidPartitionKeys(partitions)
                Set(ResourceIdentifier.ofJdbcQuery(query))
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
                dependencies.map(l => ResourceIdentifier.ofJdbcTable(TableIdentifier(l))).toSet
            case Operation.READ =>
                dependencies.map(l => ResourceIdentifier.ofJdbcTablePartition(TableIdentifier(l), Map.empty)).toSet
            case Operation.WRITE => Set.empty
        }
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

        logger.info(s"Reading JDBC query relation '$identifier' with a custom query via connection '$connection' partition $partitions")

        // Get Connection
        val props = createConnectionProperties()

        // Read from database. We do not use this.reader, because Spark JDBC sources do not support explicit schemas
        val reader = execution.spark.read
            .format("jdbc")
            .options(props)

        val tableDf = reader
            .option(JDBCOptions.JDBC_QUERY_STRING, query)
            .load()

        val filteredDf = filterPartition(tableDf, partitions)

        // Apply embedded schema, if it is specified. This will remove/cast any columns not present in the
        // explicit schema specification of the relation
        applyInputSchema(execution, filteredDf)
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
        throw new UnsupportedOperationException(s"Cannot write into JDBC query relation '$identifier' which is defined by an SQL query")
    }

    /**
      * Removes one or more partitions.
     *
     * @param execution
      * @param partitions
      */
    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
        throw new UnsupportedOperationException(s"Cannot clean JDBC query relation '$identifier' which is defined by an SQL query")
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
    override def conforms(execution: Execution): Trilean = {
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
    override def create(execution:Execution) : Unit = {
        throw new UnsupportedOperationException(s"Cannot create JDBC query relation '$identifier' which is defined by an SQL query")
    }

    /**
      * This method will physically destroy the corresponding relation in the target JDBC database.
      * @param execution
      */
    override def destroy(execution:Execution) : Unit = {
        throw new UnsupportedOperationException(s"Cannot destroy JDBC query relation '$identifier' which is defined by an SQL query")
    }

    override def migrate(execution:Execution) : Unit = {
        throw new UnsupportedOperationException(s"Cannot migrate JDBC query relation '$identifier' which is defined by an SQL query")
    }

    override protected def createConnectionProperties() : Map[String,String] = {
        val props = super.createConnectionProperties()
        props + (JDBCOptions.JDBC_QUERY_STRING -> query)
    }

    private lazy val dependencies : Seq[String] = {
        val statement = CCJSqlParserUtil.parse(query)
        val tablesNamesFinder = new TablesNamesFinder()
        tablesNamesFinder.getTableList(statement).asScala
    }

    private lazy val query: String = {
        sql
            .orElse(file.map { f =>
                FileUtils.toString(f)
            })
            .getOrElse(
                throw new IllegalArgumentException("Require either 'sql' or 'file' property")
            )
    }
}



class JdbcQueryRelationSpec extends RelationSpec with PartitionedRelationSpec with SchemaRelationSpec {
    @JsonProperty(value = "connection", required = true) private var connection: ConnectionReferenceSpec = _
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map.empty
    @JsonPropertyDescription("SQL query for the view definition. This has to be specified in database specific SQL syntax.")
    @JsonProperty(value = "sql", required = false) private var sql: Option[String] = None
    @JsonPropertyDescription("Name of a file containing the SQL query for the view definition. This has to be specified in database specific SQL syntax.")
    @JsonProperty(value = "file", required = false) private var file: Option[String] = None

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
            context.evaluate(sql),
            context.evaluate(file).map(p => context.fs.file(p)),
            context.evaluate(properties)
        )
    }
}
