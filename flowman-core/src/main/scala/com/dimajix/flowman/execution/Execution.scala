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

package com.dimajix.flowman.execution

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.SparkSession

import com.dimajix.flowman.catalog.Catalog
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.metric.MetricSystem
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
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
    def metrics : MetricSystem

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
      * Returns true if a SparkSession is already available
      * @return
      */
    def sparkRunning: Boolean

    /**
      * Returns the table catalog used for managing Hive table instances. The Catalog will take care of many
      * technical details, like refreshing additional external catalogs like Impala.
      * @return
      */
    def catalog: Catalog

    /**
     * Returns the [[OperationManager]] of this execution, which should be the instance created by the [[Session]]
     * @return
     */
    def oeprations: OperationManager

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
     * Executes an assertion from a TestSuite. This method ensures that all inputs are instantiated correctly
     * @param assertion
     * @return
     */
    def assert(assertion:Assertion) : Seq[AssertionResult] = {
        val context = assertion.context
        val inputs = assertion.inputs
            .map(id => id -> instantiate(context.getMapping(id.mapping), id.output))
            .toMap

        assertion.execute(this, inputs)
    }

    /**
     * Returns the schema for a specific output created by a specific mapping. Note that not all mappings support
     * schema analysis beforehand. In such cases, None will be returned.
     * @param mapping
     * @param output
     * @return
     */
    def describe(mapping:Mapping, output:String) : StructType

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
      * Releases any temporary tables
      */
    def cleanup() : Unit
}
