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

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableIndex
import com.dimajix.flowman.catalog.TableType
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.annotation.RelationType
import com.dimajix.flowman.spec.connection.ConnectionReferenceSpec


case class SqlServerRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq.empty,
    connection: Reference[Connection],
    table: TableIdentifier,
    properties: Map[String,String] = Map.empty,
    mergeKey: Seq[String] = Seq.empty,
    override val primaryKey: Seq[String] = Seq.empty,
    indexes: Seq[TableIndex] = Seq.empty,
    storageFormat: Option[String] = None,
    sql: Seq[String] = Seq.empty
) extends JdbcTableRelationBase(instanceProperties, schema, partitions, connection, table, properties, mergeKey, primaryKey, indexes, sql) {
    private val tempTableIdentifier = TableIdentifier(s"##${tableIdentifier.table}_temp_staging")
    override protected val stagingIdentifier: Option[TableIdentifier] = Some(tempTableIdentifier)

    override protected def createTableDefinition(): Option[TableDefinition] = {
        schema.map { schema =>
            val pk = if (primaryKey.nonEmpty) primaryKey else schema.primaryKey
            val columns = fullSchema.get.fields
            TableDefinition(
                tableIdentifier,
                TableType.TABLE,
                columns = columns,
                comment = schema.description,
                primaryKey = pk,
                indexes = indexes,
                storageFormat = storageFormat
                // Currently partition tables are not supported on a physical level, only on a logical level
                // partitionColumnNames = partitions.map(_.name)
            )
        }
    }

    override protected def appendTable(execution: Execution, df:DataFrame, table:TableIdentifier): Unit = {
        val props = createConnectionProperties()
        this.writer(execution, df, "com.microsoft.sqlserver.jdbc.spark", Map(), SaveMode.Append)
            .options(props ++ Map("tableLock" -> "true", "mssqlIsolationLevel" -> "READ_UNCOMMITTED"))
            .option(JDBCOptions.JDBC_TABLE_NAME, table.unquotedString)
            .save()
    }

    override protected def createConnectionProperties() : Map[String,String] = {
        val props = super.createConnectionProperties()
        props + (JDBCOptions.JDBC_DRIVER_CLASS -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    }
}



@RelationType(kind="sqlserver")
class SqlServerRelationSpec extends RelationSpec with PartitionedRelationSpec with SchemaRelationSpec with IndexedRelationSpec {
    @JsonProperty(value = "connection", required = true) private var connection: ConnectionReferenceSpec = _
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map.empty
    @JsonProperty(value = "database", required = false) private var database: Option[String] = None
    @JsonProperty(value = "table", required = false) private var table: String = ""
    @JsonPropertyDescription("List of columns used for merge operations.")
    @JsonProperty(value = "mergeKey", required = false) private var mergeKey: Seq[String] = Seq.empty
    @JsonPropertyDescription("List of columns making up the primary key.")
    @JsonProperty(value = "primaryKey", required = false) private var primaryKey: Seq[String] = Seq.empty
    @JsonPropertyDescription("Storage format of table. Can be either 'rowstore' or 'columnstore'.")
    @JsonProperty(value = "storageFormat", required = false) private var storageFormat: Option[String] = None
    @JsonPropertyDescription("SQL command(s) for creating the table. This has to be specified in database specific SQL syntax.")
    @JsonProperty(value = "sql", required = false) private var sql: Seq[String] = Seq.empty

    override def instantiate(context: Context, props:Option[Relation.Properties] = None): SqlServerRelation = {
        SqlServerRelation(
            instanceProperties(context, props),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            connection.instantiate(context),
            TableIdentifier(context.evaluate(table), context.evaluate(database)),
            context.evaluate(properties),
            mergeKey.map(context.evaluate),
            primaryKey.map(context.evaluate),
            indexes.map(_.instantiate(context)),
            context.evaluate(storageFormat),
            sql.map(context.evaluate)
        )
    }
}
