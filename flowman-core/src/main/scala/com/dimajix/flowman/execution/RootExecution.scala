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

package com.dimajix.flowman.execution

import org.apache.spark.sql.SparkSession
import org.slf4j.ILoggerFactory
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.HiveCatalog
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.fs.FileSystem
import com.dimajix.flowman.metric.MetricBoard
import com.dimajix.flowman.metric.MetricSystem
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.LifecycleResult
import com.dimajix.flowman.model.Measure
import com.dimajix.flowman.model.MeasureResult
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetResult


final class RootExecution(val session:Session) extends CachingExecution(None, true) {
    override protected val logger = session.loggerFactory.getLogger(classOf[RootExecution].getName)

    /**
     * Returns the MetricRegistry of this execution
     * @return
     */
    override def metricSystem : MetricSystem = session.metrics

    /**
     * Returns the currently used [[MetricBoard]] for collecting metrics
     * @return
     */
    override def metricBoard : Option[MetricBoard] = None

    /**
     * Returns the list of [[ExecutionListener]] used for monitoring the whole execution
     * @return
     */
    override def listeners : Seq[(ExecutionListener,Option[Token])] = session.listeners

    /**
     * Returns the FileSystem as configured in Hadoop
     * @return
     */
    override def fs : FileSystem = session.fs

    /**
     * Returns (or lazily creates) a SparkSession of this Executor. The SparkSession will be derived from the global
     * SparkSession, but a new derived session with a separate namespace will be created.
     *
     * @return
     */
    override def spark: SparkSession = session.spark

    /**
     * Returns true if a SparkSession is already available
     * @return
     */
    override def sparkRunning: Boolean = session.sparkRunning

    /**
     * Returns the FlowmanConf object, which contains all Flowman settings.
     * @return
     */
    override def flowmanConf : FlowmanConf = session.flowmanConf

    /**
     * Returns the table catalog used for managing table instances
     * @return
     */
    override def catalog: HiveCatalog = session.catalog

    /**
     * Returns the [[ActivityManager]] of this execution, which is the same instance created by the [[Session]]
     *
     * @return
     */
    override def activities: ActivityManager = session.activities

    override def loggerFactory: ILoggerFactory = session.loggerFactory

    /**
     * Returns the [[SessionCleaner]] provided by the [[Session]]
     *
     * @return
     */
    def cleaner : SessionCleaner = session.cleaner

    /**
     * Releases any temporary tables
     */
    override def cleanup() : Unit = {
        if (sparkRunning) {
            logger.info("Cleaning up cached Spark tables")
            val catalog = spark.catalog
            catalog.clearCache()
        }

        super.cleanup()
    }
}
