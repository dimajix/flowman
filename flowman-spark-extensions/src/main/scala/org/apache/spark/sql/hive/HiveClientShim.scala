/*
 * Copyright 2021 Kaya Kupferschmidt
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

package org.apache.spark.sql.hive

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogWithListener


object HiveClientShim {
    private var _currentSparkSession:SparkSession = null
    private var _currentHive:Hive = null
    private var _currentListener:SparkListener = null

    /** Run a function within Hive state (SessionState, HiveConf, Hive client and class loader) */
    def withHiveState[A](spark:SparkSession)(f: => A): A = {
        val catalog = hiveCatalog(spark)
        catalog.client.withHiveState(f)
    }

    def withHiveSession[A](spark:SparkSession)(f: => A): A = {
        // Check if the given Spark session is the same as last time. If so, then do not create a new Hive client.
        if (_currentSparkSession ne spark) {
            if (_currentHive != null) {
                assert(_currentSparkSession != null)
                assert(_currentSparkSession != null)

                // Remove Spark listener again
                _currentSparkSession.sparkContext.removeSparkListener(_currentListener)
                _currentListener = null

                if (Hive.getThreadLocal == _currentHive) {
                    Hive.closeCurrent()
                }
                else {
                    _currentHive.close(false)
                }
                _currentHive = null
            }

            // Create new Hive Session
            val conf = new HiveConf(spark.sparkContext.hadoopConfiguration, classOf[Hive])
            val hive = Hive.get(conf)

            // Release Hive session when Spark Context is destroyed
            val listener = new SparkListener {
                override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
                    if (Hive.getThreadLocal() == hive) {
                        Hive.closeCurrent()
                    }
                    else {
                        hive.close(false)
                    }
                }
            }
            spark.sparkContext.addSparkListener(listener)

            // Set Hive session
            Hive.set(hive)
            _currentSparkSession = spark
            _currentHive = hive
            _currentListener = listener
        }
        f
    }

    private def hiveCatalog(spark:SparkSession) : HiveExternalCatalog = {
        def unwrap(catalog:ExternalCatalog) : HiveExternalCatalog = catalog match {
            case c: ExternalCatalogWithListener => unwrap(c.unwrapped)
            case h: HiveExternalCatalog => h
        }
        unwrap(spark.sessionState.catalog.externalCatalog)
    }
}
