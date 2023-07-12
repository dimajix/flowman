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

package com.dimajix.flowman.execution

import org.apache.spark.sql.SparkSession
import org.slf4j.ILoggerFactory
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.HiveCatalog
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.fs.FileSystem
import com.dimajix.flowman.metric.MetricBoard
import com.dimajix.flowman.metric.MetricSystem


/**
 * The ScopedExecution is a caching execution with the added ability to isolate resources (i.e. not to reuse
 * existing DataFrames) and with a separate [[ActivityManager]] for capturing background activities.
 *
 * @param parent
 * @param isolated
 */
final class ScopedExecution(parent:Execution, isolated:Boolean=true) extends CachingExecution(Some(parent), isolated) {
    override protected val logger = parent.loggerFactory.getLogger(classOf[ScopedExecution].getName)
    private val operationsManager = new ActivityManager(parent.activities)

    /**
     * Returns the FlowmanConf object, which contains all Flowman settings.
     * @return
     */
    override def flowmanConf : FlowmanConf = parent.flowmanConf

    /**
     * Returns the MetricRegistry of this execution
     * @return
     */
    override def metricSystem : MetricSystem = parent.metricSystem

    override def metricBoard : Option[MetricBoard] = parent.metricBoard

    override def listeners : Seq[(ExecutionListener,Option[Token])] = parent.listeners

    /**
     * Returns the FileSystem as configured in Hadoop
     * @return
     */
    override def fs : FileSystem = parent.fs

    /**
     * Returns (or lazily creates) a SparkSession of this Executor. The SparkSession will be derived from the global
     * SparkSession, but a new derived session with a separate namespace will be created.
     *
     * @return
     */
    override def spark: SparkSession = parent.spark

    /**
     * Returns the table catalog used for managing table instances
     * @return
     */
    override def catalog: HiveCatalog = parent.catalog

    /**
     * Returns the [[ActivityManager]] of this execution, which should be the instance created by the [[Session]]
     *
     * @return
     */
    override def activities: ActivityManager = operationsManager

    override def loggerFactory: ILoggerFactory = parent.loggerFactory

    /**
     * Returns the [[SessionCleaner]] provided by the [[Session]]
     *
     * @return
     */
    override def cleaner: SessionCleaner = parent.cleaner
}
