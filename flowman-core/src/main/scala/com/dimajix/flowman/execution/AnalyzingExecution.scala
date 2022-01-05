/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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
import org.apache.spark.sql.SparkSession.getActiveSession
import org.apache.spark.sql.SparkSession.getDefaultSession
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.HiveCatalog
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.hadoop.FileSystem
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


class AnalyzingExecution(context: Context) extends CachingExecution(None, true) {
    override protected val logger = LoggerFactory.getLogger(classOf[AnalyzingExecution])

    private lazy val _metricSystem = new MetricSystem

    /**
     * Returns the FlowmanConf object, which contains all Flowman settings.
     * @return
     */
    def flowmanConf : FlowmanConf = context.flowmanConf

    /**
     * Returns the MetricRegistry of this execution
     *
     * @return
     */
    override def metrics: MetricSystem = _metricSystem

    /**
     * Returns the FileSystem as configured in Hadoop
     *
     * @return
     */
    override def fs: FileSystem = context.fs

    /**
     * Returns (or lazily creates) a SparkSession of this Executor. The SparkSession will be derived from the global
     * SparkSession, but a new derived session with a separate namespace will be created.
     *
     * @return
     */
    override def spark: SparkSession = getActiveSession.getOrElse(getDefaultSession.getOrElse(
        throw new IllegalStateException("No active or default Spark session found")))

    /**
     * Returns true if a SparkSession is already available
     *
     * @return
     */
    override def sparkRunning: Boolean = true

    /**
     * Returns the table catalog used for managing table instances
     *
     * @return
     */
    override def catalog: HiveCatalog = throw new UnsupportedOperationException

    /**
     * Executes an assertion from a TestSuite. This method ensures that all inputs are instantiated correctly
     *
     * @param assertion
     * @return
     */
    override def assert(assertion: Assertion): AssertionResult = throw new UnsupportedOperationException

    /**
     * Returns the [[OperationManager]] of this execution, which should be the instance created by the [[Session]]
     *
     * @return
     */
    override def operations: OperationManager = throw new UnsupportedOperationException

    override def withListeners[T](listeners: Seq[ExecutionListener])(fn: Execution => T): T = throw new UnsupportedOperationException

    override def withMetrics[T](metrics: Option[MetricBoard])(fn: Execution => T): T = throw new UnsupportedOperationException

    override def monitorLifecycle(job: Job, arguments: Map[String, Any], phases: Seq[Phase])(fn: Execution => LifecycleResult): LifecycleResult = throw new UnsupportedOperationException

    override def monitorJob(job: Job, arguments: Map[String, Any], phase: Phase)(fn: Execution => JobResult): JobResult = throw new UnsupportedOperationException

    override def monitorTarget(target: Target, phase: Phase)(fn: Execution => TargetResult): TargetResult = throw new UnsupportedOperationException

    override def monitorAssertion(assertion: Assertion)(fn: Execution => AssertionResult): AssertionResult = throw new UnsupportedOperationException

    override def monitorMeasure(measure:Measure)(fn:Execution => MeasureResult) : MeasureResult = throw new UnsupportedOperationException
}
