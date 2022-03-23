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

import scala.collection.mutable

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import com.dimajix.flowman.catalog
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableIndex
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.jdbc.JdbcUtils
import com.dimajix.flowman.jdbc.SqlDialects
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.annotation.RelationType
import com.dimajix.flowman.spec.connection.ConnectionReferenceSpec
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.types.StructType


case class SqlServerRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq.empty,
    connection: Reference[Connection],
    properties: Map[String,String] = Map.empty,
    table: Option[TableIdentifier] = None,
    query: Option[String] = None,
    mergeKey: Seq[String] = Seq.empty,
    override val primaryKey: Seq[String] = Seq.empty,
    indexes: Seq[TableIndex] = Seq.empty
) extends JdbcRelationBase(instanceProperties, schema, partitions, connection, properties, table, query, mergeKey, primaryKey, indexes) {
    private val tempTableIdentifier = TableIdentifier(s"##${tableIdentifier.table}_temp_staging")
    override protected val stagingIdentifier: Option[TableIdentifier] = Some(tempTableIdentifier)

    override protected def appendTable(execution: Execution, df:DataFrame, table:TableIdentifier): Unit = {
        val (_,props) = createConnectionProperties()
        this.writer(execution, df, "com.microsoft.sqlserver.jdbc.spark", Map(), SaveMode.Append)
            .options(props ++ Map("tableLock" -> "true", "mssqlIsolationLevel" -> "READ_UNCOMMITTED"))
            .option(JDBCOptions.JDBC_TABLE_NAME, table.unquotedString)
            .save()
    }

    override protected def createConnectionProperties() : (String,Map[String,String]) = {
        val connection = this.connection.value.asInstanceOf[JdbcConnection]
        val props = mutable.Map[String,String]()
        props.put(JDBCOptions.JDBC_URL, connection.url)
        props.put(JDBCOptions.JDBC_DRIVER_CLASS, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        connection.username.foreach(props.put("user", _))
        connection.password.foreach(props.put("password", _))

        connection.properties.foreach(kv => props.put(kv._1, kv._2))
        properties.foreach(kv => props.put(kv._1, kv._2))

        (connection.url,props.toMap)
    }
}



@RelationType(kind="sqlserver")
class SqlServerRelationSpec extends RelationSpec with PartitionedRelationSpec with SchemaRelationSpec with IndexedRelationSpec {
    @JsonProperty(value = "connection", required = true) private var connection: ConnectionReferenceSpec = _
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map.empty
    @JsonProperty(value = "database", required = false) private var database: Option[String] = None
    @JsonProperty(value = "table", required = false) private var table: Option[String] = None
    @JsonProperty(value = "query", required = false) private var query: Option[String] = None
    @JsonProperty(value = "mergeKey", required = false) private var mergeKey: Seq[String] = Seq.empty
    @JsonProperty(value = "primaryKey", required = false) private var primaryKey: Seq[String] = Seq.empty

    override def instantiate(context: Context, props:Option[Relation.Properties] = None): SqlServerRelation = {
        new SqlServerRelation(
            instanceProperties(context, props),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            connection.instantiate(context),
            context.evaluate(properties),
            context.evaluate(table).map(t => TableIdentifier(t, context.evaluate(database))),
            context.evaluate(query),
            mergeKey.map(context.evaluate),
            primaryKey.map(context.evaluate),
            indexes.map(_.instantiate(context))
        )
    }
}
