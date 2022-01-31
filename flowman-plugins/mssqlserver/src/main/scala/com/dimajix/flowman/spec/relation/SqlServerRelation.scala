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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.annotation.RelationType
import com.dimajix.flowman.spec.connection.ConnectionReferenceSpec
import com.dimajix.flowman.spec.connection.JdbcConnection


class SqlServerRelation(
    instanceProperties:Relation.Properties,
    schema:Option[Schema] = None,
    partitions: Seq[PartitionField] = Seq(),
    connection: Reference[Connection],
    properties: Map[String,String] = Map(),
    database: Option[String] = None,
    table: Option[String] = None,
    query: Option[String] = None,
    mergeKey: Seq[String] = Seq(),
    primaryKey: Seq[String] = Seq()
) extends JdbcRelation(instanceProperties, schema, partitions, connection, properties, database, table, query, mergeKey, primaryKey) {
    override protected def doWrite(execution: Execution, df:DataFrame): Unit = {
        val (_,props) = createConnectionProperties()
        this.writer(execution, df, "com.microsoft.sqlserver.jdbc.spark", Map(), SaveMode.Append)
            .options(props)
            .option(JDBCOptions.JDBC_TABLE_NAME, tableIdentifier.unquotedString)
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
class SqlServerRelationSpec extends RelationSpec with PartitionedRelationSpec with SchemaRelationSpec {
    @JsonProperty(value = "connection", required = true) private var connection: ConnectionReferenceSpec = _
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map()
    @JsonProperty(value = "database", required = false) private var database: Option[String] = None
    @JsonProperty(value = "table", required = false) private var table: Option[String] = None
    @JsonProperty(value = "query", required = false) private var query: Option[String] = None
    @JsonProperty(value = "mergeKey", required = false) private var mergeKey: Seq[String] = Seq()
    @JsonProperty(value = "primaryKey", required = false) private var primaryKey: Seq[String] = Seq()

    override def instantiate(context: Context): SqlServerRelation = {
        new SqlServerRelation(
            instanceProperties(context),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            connection.instantiate(context),
            context.evaluate(properties),
            database.map(context.evaluate).filter(_.nonEmpty),
            table.map(context.evaluate).filter(_.nonEmpty),
            query.map(context.evaluate).filter(_.nonEmpty),
            mergeKey.map(context.evaluate),
            primaryKey.map(context.evaluate)
        )
    }
}
