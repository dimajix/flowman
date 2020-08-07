/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.Catalog
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.metric.MetricSystem


class ScopedExecutor(parent:Executor) extends CachingExecutor(Some(parent), true) {
    override protected val logger = LoggerFactory.getLogger(classOf[ScopedExecutor])

    /**
     * Returns the FlowmanConf object, which contains all Flowman settings.
     * @return
     */
    def flowmanConf : FlowmanConf = parent.flowmanConf

    /**
     * Returns the MetricRegistry of this executor
     * @return
     */
    override def metrics : MetricSystem = parent.metrics

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
     * Returns true if a SparkSession is already available
     * @return
     */
    override def sparkRunning: Boolean = parent.sparkRunning

    /**
     * Returns the table catalog used for managing table instances
     * @return
     */
    override def catalog: Catalog = parent.catalog
}
