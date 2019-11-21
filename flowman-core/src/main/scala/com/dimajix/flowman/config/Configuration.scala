/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.config

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkShim


class Configuration(userSettings:Map[String,String]) {
    private val systemSettings =
        System.getProperties.stringPropertyNames().asScala.filter(_.startsWith("flowman."))
            .map(key => (key, System.getProperty(key)))
            .toMap

    private val allSettings = userSettings ++ systemSettings
    private val flowmanSettings = allSettings.filterKeys(isFlowmanSetting)
    private val sparkSettings = allSettings.filterKeys(!isFlowmanSetting(_))

    private def isFlowmanSetting(key:String) : Boolean = key.startsWith("flowman.")

    /**
     * This variable contains Flowman configuration object
     */
    val flowmanConf:FlowmanConf = new FlowmanConf(flowmanSettings)
    /**
     * Spark configuration also derived from all global settings
     */
    val sparkConf:SparkConf = new SparkConf().setAll(sparkSettings)
    /**
     * Hadoop configuration constructed from the SparkConf
     */
    val hadoopConf:org.apache.hadoop.conf.Configuration = SparkShim.getHadoopConf(sparkConf)

    /**
     * Returns the value of runtime configuration property for the given key.
     *
     * @throws java.util.NoSuchElementException if the key is not set and does not have a default
     *                                          value
     */
    @throws[NoSuchElementException]("if the key is not set")
    def get(key: String): String = {
        if (isFlowmanSetting(key)) {
            flowmanConf.get(key)
        }
        else {
            sparkConf.get(key)
        }
    }

    /**
     * Returns the value of Spark runtime configuration property for the given key.
     */
    def get(key: String, default: String): String = {
        if (isFlowmanSetting(key)) {
            flowmanConf.get(key, default)
        }
        else {
            sparkConf.get(key, default)
        }
    }

    /**
     * Returns the value of Spark runtime configuration property for the given key.
     */
    def getOption(key: String): Option[String] = {
        try Option(get(key)) catch {
            case _: NoSuchElementException => None
        }
    }

    /**
     * Returns whether a particular key is set.
     */
    def contains(key: String): Boolean = {
        if (isFlowmanSetting(key)) {
            flowmanConf.contains(key)
        }
        else {
            sparkConf.contains(key)
        }
    }
}
