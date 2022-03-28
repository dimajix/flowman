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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.SparkSession

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
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Measure
import com.dimajix.flowman.model.MeasureResult
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.StructType


/**
 * An [[Execution]] is some sort of an execution context that is able to create DataFrames from Mappings. It also
 * provides access to the Spark session, Hadoop filesystem and configurations. The [[Execution]] is only used during
 * target execution and not created before.
 */
abstract class Execution {
    /**
      * Returns the MetricRegistry of this execution
      * @return
      */
    def metricSystem : MetricSystem

    /**
     * Returns the currently used [[MetricBoard]] for collecting metrics
     * @return
     */
    def metricBoard : Option[MetricBoard]

    /**
     * Returns the list of [[ExecutionListener]] used for monitoring the whole execution
     * @return
     */
    def listeners : Seq[(ExecutionListener,Option[Token])]

    /**
      * Returns the FileSystem as configured in Hadoop
      * @return
      */
    def fs : FileSystem

    /**
      * Returns (or lazily creates) a SparkSession of this Executor. The SparkSession will be derived from the global
      * SparkSession, but a new derived session with a separate namespace will be created.
      *
      * @return
      */
    def spark: SparkSession

    /**
     * Returns true if a SparkSession is already available
     * @return
     */
    def sparkRunning: Boolean

    /**
     * Returns the FlowmanConf object, which contains all Flowman settings.
     * @return
     */
    def flowmanConf : FlowmanConf

    /**
      * Returns the Spark configuration
      */
    def sparkConf : RuntimeConfig = spark.conf

    /**
      * Returns the Hadoop configuration as used by Spark
      * @return
      */
    def hadoopConf : Configuration = spark.sparkContext.hadoopConfiguration

    /**
      * Returns the table catalog used for managing Hive table instances. The HiveCatalog will take care of many
      * technical details, like refreshing additional external catalogs like Impala.
      * @return
      */
    def catalog: HiveCatalog

    /**
     * Returns the [[OperationManager]] of this execution, which should be the instance created by the [[Session]]
     * @return
     */
    def operations: OperationManager

    /**
      * Creates an instance of a mapping, or retrieves it from cache
      *
      * @param mapping
      */
    def instantiate(mapping:Mapping) : Map[String,DataFrame]

    /**
      * Creates an instance of a mapping, or retrieves it from cache
      *
      * @param mapping
      */
    def instantiate(mapping:Mapping, output:String) : DataFrame = {
        if (!mapping.outputs.contains(output))
            throw new NoSuchMappingOutputException(MappingOutputIdentifier(mapping.identifier.name, output, mapping.identifier.project))

        val instances = instantiate(mapping)
        instances(output)
    }

    /**
     * Executes an [[Assertion]] from a TestSuite. This method ensures that all inputs are instantiated correctly
     * @param assertion
     * @return
     */
    def assert(assertion:Assertion) : AssertionResult = {
        val context = assertion.context
        val inputs = assertion.inputs
            .map(id => id -> instantiate(context.getMapping(id.mapping), id.output))
            .toMap

        assertion.execute(this, inputs)
    }

    /**
     * Executes a [[Measure]]. This method ensures that all inputs are instantiated correctly.
     * @param measure
     * @return
     */
    def measure(measure:Measure) : MeasureResult = {
        val context = measure.context
        val inputs = measure.inputs
            .map(id => id -> instantiate(context.getMapping(id.mapping), id.output))
            .toMap

        measure.execute(this, inputs)
    }

    /**
     * Returns the schema for a specific output created by a specific mapping. Note that not all mappings support
     * schema analysis beforehand. In such cases, None will be returned.
     * @param mapping
     * @param output
     * @return
     */
    def describe(mapping:Mapping, output:String) : StructType = {
        if (!mapping.outputs.contains(output))
            throw new NoSuchMappingOutputException(mapping.identifier, output)

        describe(mapping)(output)
    }

    /**
     * Returns the schema for a specific output created by a specific mapping.
     * @param mapping
     * @return
     */
    def describe(mapping:Mapping) : Map[String, StructType] = {
        val context = mapping.context
        val deps = mapping.inputs
            .map(id => id -> describe(context.getMapping(id.mapping), id.output))
            .toMap

        mapping.describe(this, deps)
    }

    /**
     * Returns the schema for a specific relation
     * @param relation
     * @param partitions
     * @return
     */
    def describe(relation:Relation, partitions:Map[String,FieldValue] = Map()) : StructType

    /**
     * Registers a refresh function associated with a [[ResourceIdentifier]]
     * @param key
     * @param refresh
     */
    def addResource(key:ResourceIdentifier)(refresh: => Unit) : Unit

    /**
     * Invokes all refresh functions associated with a [[ResourceIdentifier]]
     * @param key
     */
    def refreshResource(key:ResourceIdentifier) : Unit

    /**
      * Releases any temporary tables
      */
    def cleanup() : Unit

    /**
     * Invokes a function with a new [[Executor]] that with additional [[ExecutionListener]].
     * @param listeners
     * @param fn
     * @tparam T
     * @return
     */
    def withListeners[T](listeners:Seq[ExecutionListener])(fn:Execution => T) : T

    /**
     * Invokes a function with a new Executor with a specific [[MetricBoard]]
     * @param metrics
     * @param fn
     * @tparam T
     * @return
     */
    def withMetrics[T](metrics:Option[MetricBoard])(fn:Execution => T) : T

    /**
     * Monitors the execution of a lifecycle by calling appropriate listeners at the start and end.
     * @param job
     * @param arguments
     * @param phases
     * @param fn
     * @tparam T
     * @return
     */
    def monitorLifecycle(job:Job, arguments:Map[String,Any], phases:Seq[Phase])(fn:Execution => LifecycleResult) : LifecycleResult

    /**
     * Monitors the execution of a job by calling appropriate listeners at the start and end.
     * @param job
     * @param arguments
     * @param phase
     * @param fn
     * @tparam T
     * @return
     */
    def monitorJob(job:Job, arguments:Map[String,Any], phase:Phase)(fn:Execution => JobResult) : JobResult

    /**
     * Monitors the execution of a target by calling appropriate listeners at the start and end.
     * @param target
     * @param phase
     * @param fn
     * @tparam T
     * @return
     */
    def monitorTarget(target:Target, phase:Phase)(fn:Execution => TargetResult) : TargetResult

    /**
     * Monitors the execution of an assertion by calling appropriate listeners at the start and end.
     * @param assertion
     * @param fn
     * @tparam T
     * @return
     */
    def monitorAssertion(assertion:Assertion)(fn:Execution => AssertionResult) : AssertionResult

    /**
     * Monitors the execution of an assertion by calling appropriate listeners at the start and end.
     * @param assertion
     * @param fn
     * @tparam T
     * @return
     */
    def monitorMeasure(measure:Measure)(fn:Execution => MeasureResult) : MeasureResult
}
