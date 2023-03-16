/*
 * Copyright (C) 2018 The Flowman Authors
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

package org.apache.spark.sql

import java.sql.Connection
import java.util.TimeZone

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.GroupingSets
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.AlterViewAsCommand
import org.apache.spark.sql.execution.command.CreateDatabaseCommand
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.execution.command.ViewType
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.CalendarInterval

import com.dimajix.util.DateTimeUtils


object SparkShim {
    def getHadoopConf(sparkConf:SparkConf) :org.apache.hadoop.conf.Configuration = SparkHadoopUtil.get.newConfiguration(sparkConf)

    def parseCalendarInterval(str:String) : CalendarInterval = CalendarInterval.fromString(str)

    def calendarInterval(months:Int, days:Int, microseconds:Long=0L) : CalendarInterval = {
        new CalendarInterval(months, microseconds + days*DateTimeUtils.MICROS_PER_DAY)
    }

    def millisToDays(millisUtc: Long, timeZone: TimeZone): Int = {
        org.apache.spark.sql.catalyst.util.DateTimeUtils.millisToDays(millisUtc, timeZone)
    }

    def isStaticConf(key:String) : Boolean = {
        SQLConf.staticConfKeys.contains(key)
    }

    def relationSupportsMultiplePaths(spark:SparkSession, format:String) : Boolean = {
        val providingClass = DataSource.lookupDataSource(format, spark.sessionState.conf)
        relationSupportsMultiplePaths(providingClass)
    }

    def relationSupportsMultiplePaths(providingClass:Class[_]) : Boolean = {
        providingClass.getDeclaredConstructor().newInstance() match {
            case _: RelationProvider => false
            case _: SchemaRelationProvider => false
            case _: FileFormat => true
            case _ => false
        }
    }

    def groupingSetAggregate(
        groupByExpressions:Seq[Expression],
        groupingSets:Seq[Seq[Expression]],
        aggregateExpressions: Seq[NamedExpression],
        child: LogicalPlan) : LogicalPlan = {
        GroupingSets(
            groupingSets,
            groupByExpressions,
            child,
            aggregateExpressions
        )
    }

    def withNewExecutionId[T](
        sparkSession: SparkSession,
        queryExecution: QueryExecution,
        name: Option[String] = None)(body: => T): T =
        SQLExecution.withNewExecutionId(sparkSession, queryExecution)(body)

    def functionRegistry(spark:SparkSession) : FunctionRegistry = spark.sessionState.functionRegistry

    def newCreateViewCommand(table:TableIdentifier, select:String, plan:LogicalPlan, allowExisting:Boolean, replace:Boolean) : CreateViewCommand = {
        CreateViewCommand(table, Nil, None, Map(), Some(select), plan, allowExisting, replace, SparkShim.PersistedView)
    }
    def newAlterViewCommand(table:TableIdentifier, select:String, plan:LogicalPlan) : AlterViewAsCommand = {
        AlterViewAsCommand(table, select, plan)
    }
    def newCreateDatabaseCommand(database: String, catalog: String, path: Option[String], comment: Option[String], ignoreIfExists: Boolean): CreateDatabaseCommand = {
        CreateDatabaseCommand(database, ignoreIfExists, path, comment, Map())
    }
    def newCatalogTable(
        identifier: TableIdentifier,
        tableType: CatalogTableType,
        storage: CatalogStorageFormat,
        schema: StructType,
        provider: Option[String] = None,
        partitionColumnNames: Seq[String] = Seq.empty,
        bucketSpec: Option[BucketSpec] = None,
        properties: Map[String, String] = Map.empty,
        comment: Option[String] = None): CatalogTable = {
        CatalogTable(
            identifier = identifier,
            tableType = tableType,
            storage = storage,
            schema = schema,
            provider = provider,
            partitionColumnNames = partitionColumnNames,
            bucketSpec = bucketSpec,
            properties = properties,
            comment = comment
        )
    }
    def withNewSchema(table: CatalogTable, schema: StructType): CatalogTable = {
        table.copy(schema = schema)
    }

    def createConnectionFactory(dialect: JdbcDialect, options: JDBCOptions) :  Int => Connection = {
        val factory = JdbcUtils.createConnectionFactory(options)
        (_:Int) => factory()
    }
    def savePartition(
        table: String,
        iterator: Iterator[Row],
        rddSchema: StructType,
        insertStmt: String,
        batchSize: Int,
        dialect: JdbcDialect,
        isolationLevel: Int,
        options: JDBCOptions): Unit = {
        val getConnection: () => Connection = JdbcUtils.createConnectionFactory(options)
        JdbcUtils.savePartition(getConnection, table, iterator, rddSchema, insertStmt, batchSize, dialect, isolationLevel, options)
    }

    def alias(col: Column, alias: String, metadata: Metadata, nonInheritableMetadataKeys: Seq[String]): Column = {
        Column(Alias(col.expr, alias)(explicitMetadata = Some(metadata)))
    }

    def observe[T](ds: Dataset[T], name: String, expr: Column, exprs: Column*): Dataset[T] = {
        ds
    }
    def observedMetrics(qe: QueryExecution): Map[String, Row] = {
        Map.empty
    }


    def explainString[T](ds: Dataset[T], extended: Boolean): String = {
        val explain = ExplainCommand(ds.queryExecution.logical, extended = extended)
        ds.sparkSession.sessionState.executePlan(explain)
            .executedPlan
            .executeCollect()
            .map(_.getString(0))
            .reduce(_ + "\n" + _)
    }


    val LocalTempView : ViewType = org.apache.spark.sql.execution.command.LocalTempView
    val GlobalTempView : ViewType = org.apache.spark.sql.execution.command.GlobalTempView
    val PersistedView : ViewType = org.apache.spark.sql.execution.command.PersistedView
}
