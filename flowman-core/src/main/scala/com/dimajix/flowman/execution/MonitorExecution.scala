/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import com.dimajix.flowman.catalog.HiveCatalog
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.metric.MetricBoard
import com.dimajix.flowman.metric.MetricSystem
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.StructType


final class MonitorExecution(parent:Execution, override val listeners:Seq[(ExecutionListener,Option[Token])], override val metricBoard:Option[MetricBoard]) extends AbstractExecution {
    /**
     * Returns the MetricRegistry of this execution
     *
     * @return
     */
    override def metricSystem: MetricSystem = parent.metricSystem

    /**
     * Returns the FileSystem as configured in Hadoop
     *
     * @return
     */
    override def fs: FileSystem = parent.fs

    /**
     * Returns (or lazily creates) a SparkSession of this Executor. The SparkSession will be derived from the global
     * SparkSession, but a new derived session with a separate namespace will be created.
     *
     * @return
     */
    override def spark: SparkSession = parent.spark

    /**
     * Returns the FlowmanConf object, which contains all Flowman settings.
     *
     * @return
     */
    override def flowmanConf: FlowmanConf = parent.flowmanConf

    /**
     * Returns true if a SparkSession is already available
     *
     * @return
     */
    override def sparkRunning: Boolean = parent.sparkRunning

    /**
     * Returns the table catalog used for managing Hive table instances. The HiveCatalog will take care of many
     * technical details, like refreshing additional external catalogs like Impala.
     *
     * @return
     */
    override def catalog: HiveCatalog = parent.catalog

    /**
     * Returns the [[OperationManager]] of this execution, which should be the instance created by the [[Session]]
     *
     * @return
     */
    override def operations: OperationManager = parent.operations

    /**
     * Creates an instance of a mapping, or retrieves it from cache
     *
     * @param mapping
     */
    override def instantiate(mapping: Mapping): Map[String, DataFrame] = parent.instantiate(mapping)

    /**
     * Creates an instance of a mapping, or retrieves it from cache
     *
     * @param mapping
     */
    override def instantiate(mapping: Mapping, output: String): DataFrame = parent.instantiate(mapping, output)

    /**
     * Returns the schema for a specific output created by a specific mapping.
     *
     * @param mapping
     * @return
     */
    override def describe(mapping: Mapping): Map[String, StructType] = parent.describe(mapping)

    /**
     * Returns the schema for a specific output created by a specific mapping. Note that not all mappings support
     * schema analysis beforehand. In such cases, None will be returned.
     *
     * @param mapping
     * @param output
     * @return
     */
    override def describe(mapping: Mapping, output: String): StructType = parent.describe(mapping, output)

    /**
     * Returns the schema for a specific relation
     * @param relation
     * @param partitions
     * @return
     */
    override def describe(relation:Relation, partitions:Map[String,FieldValue] = Map()) : StructType = parent.describe(relation, partitions)

    override def addResource(key:ResourceIdentifier)(refresh: => Unit) : Unit = parent.addResource(key)(refresh)
    override def refreshResource(key:ResourceIdentifier) : Unit = parent.refreshResource(key)

    /**
     * Releases any temporary tables
     */
    override def cleanup(): Unit = parent.cleanup()

    /**
     * The delegate is responsible for doing the actual work in the context of monitoring.
     * @return
     */
    override protected def delegate : Execution = parent
}
