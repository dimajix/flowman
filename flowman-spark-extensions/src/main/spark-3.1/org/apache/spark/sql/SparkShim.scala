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

package org.apache.spark.sql

import java.time.Instant
import java.time.LocalDateTime
import java.util.TimeZone

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.analysis.ViewType
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.util.DateTimeUtils.localDateToDays
import org.apache.spark.sql.catalyst.util.DateTimeUtils.microsToInstant
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.unsafe.types.UTF8String

import com.dimajix.util.DateTimeUtils


object SparkShim {
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
        SQLConf.staticConfKeys.contains(key) ||
            (ConfigEntry.findEntry(key) != null && !SQLConf.sqlConfEntries.containsKey(key))
    }

    def relationSupportsMultiplePaths(spark:SparkSession, format:String) : Boolean = {
        val providingClass = DataSource.lookupDataSource(format, spark.sessionState.conf)
        relationSupportsMultiplePaths(providingClass)
    }

    def relationSupportsMultiplePaths(providingClass:Class[_]) : Boolean = {
        providingClass.newInstance() match {
            case _: RelationProvider => false
            case _: SchemaRelationProvider => false
            case _: FileFormat => true
            case _: FileDataSourceV2 => true
            case _ => false
        }
    }

    def withNewExecutionId[T](
        sparkSession: SparkSession,
        queryExecution: QueryExecution,
        name: Option[String] = None)(body: => T): T =
        SQLExecution.withNewExecutionId(queryExecution, name)(body)

    val LocalTempView : ViewType = org.apache.spark.sql.catalyst.analysis.LocalTempView
    val GlobalTempView : ViewType = org.apache.spark.sql.catalyst.analysis.GlobalTempView
    val PersistedView : ViewType = org.apache.spark.sql.catalyst.analysis.PersistedView
}
