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
import java.time.Instant
import java.time.LocalDateTime
import java.util.TimeZone

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.ViewType
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GroupingSets
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.BadRecordException
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.execution.ExtendedMode
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.SimpleMode
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.command.AlterViewAsCommand
import org.apache.spark.sql.execution.command.CreateDatabaseCommand
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

import com.dimajix.util.DateTimeUtils


class SparkShim
object SparkShim {
    private val logger = LoggerFactory.getLogger(classOf[SparkShim])

    def getHadoopConf(sparkConf:SparkConf) :org.apache.hadoop.conf.Configuration = SparkHadoopUtil.get.newConfiguration(sparkConf)

    def parseCalendarInterval(str:String) : CalendarInterval = IntervalUtils.stringToInterval(UTF8String.fromString(str))

    def calendarInterval(months:Int, days:Int, microseconds:Long=0L) : CalendarInterval = {
        new CalendarInterval(months, days, microseconds)
    }

    def millisToDays(millisUtc: Long, timeZone: TimeZone): Int = {
        val secs = Math.floorDiv(millisUtc, DateTimeUtils.MILLIS_PER_SECOND)
        val mos = Math.floorMod(millisUtc, DateTimeUtils.MILLIS_PER_SECOND)
        val instant = Instant.ofEpochSecond(secs, mos * DateTimeUtils.NANOS_PER_MILLIS)
        Math.toIntExact(LocalDateTime.ofInstant(instant, timeZone.toZoneId()).toLocalDate.toEpochDay)
    }

    def isStaticConf(key:String) : Boolean = {
        SQLConf.isStaticConfigKey(key) || (ConfigEntry.findEntry(key) != null && !SQLConf.containsConfigKey(key))
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
            case _: FileDataSourceV2 => true
            case _ => false
        }
    }

    def groupingSetAggregate(
        groupByExpressions:Seq[Expression],
        groupingSets:Seq[Seq[Expression]],
        aggregateExpressions: Seq[NamedExpression],
        child: LogicalPlan) : LogicalPlan = {
        Aggregate(
            Seq(GroupingSets(groupingSets, groupByExpressions)),
            aggregateExpressions,
            child
        )
    }

    def withNewExecutionId[T](
        sparkSession: SparkSession,
        queryExecution: QueryExecution,
        name: Option[String] = None)(body: => T): T =
        SQLExecution.withNewExecutionId(queryExecution, name)(body)

    def functionRegistry(spark:SparkSession) : FunctionRegistry = spark.sessionState.functionRegistry

    def newCreateViewCommand(table:TableIdentifier, select:String, plan:LogicalPlan, allowExisting:Boolean, replace:Boolean) : CreateViewCommand = {
        CreateViewCommand(table, Nil, None, Map(), Some(select), plan, allowExisting, replace, SparkShim.PersistedView, isAnalyzed=true)
    }
    def newAlterViewCommand(table:TableIdentifier, select:String, plan:LogicalPlan) : AlterViewAsCommand = {
        AlterViewAsCommand(table, select, plan, isAnalyzed=true)
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
        Column(Alias(col.expr, alias)(explicitMetadata = Some(metadata), nonInheritableMetadataKeys = nonInheritableMetadataKeys))
    }

    def observe[T](ds: Dataset[T], name: String, expr: Column, exprs: Column*): Dataset[T] = {
        ds.observe(name, expr, exprs: _*)
    }
    def observedMetrics(qe: QueryExecution): Map[String, Row] = {
        qe.observedMetrics
    }

    def toPath(path:String) : Path = new Path(path)

    def rowEncoderFor(schema: StructType) : Encoder[Row] = RowEncoder(schema)
    def expressionEncoderFor(schema: StructType) : Encoder[Row] = RowEncoder(schema)

    def newBadRecordException(cause: Throwable): BadRecordException = {
        throw BadRecordException(
            () => new UTF8String(),
            () => None,
            cause
        )
    }

    def extractInMemoryRelation(plan: LogicalPlan): Option[InMemoryRelation] = {
        plan match {
            case c: InMemoryRelation => Some(c)
            // The next case should not happen
            case _ =>
                logger.warn("Found wrong child type in EagerCache.")
                None
        }
    }

    def explainString[T](ds: Dataset[T], extended: Boolean): String = {
        val mode = if (extended) ExtendedMode else SimpleMode
        ds.queryExecution.explainString(mode)
    }

    val LocalTempView : ViewType = org.apache.spark.sql.catalyst.analysis.LocalTempView
    val GlobalTempView : ViewType = org.apache.spark.sql.catalyst.analysis.GlobalTempView
    val PersistedView : ViewType = org.apache.spark.sql.catalyst.analysis.PersistedView
}
