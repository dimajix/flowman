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
    private val _currentSparkSession = new ThreadLocal[SparkSession]()
    private val _currentHive = new ThreadLocal[Hive]()
    private val _currentListener= new ThreadLocal[SparkListener]()

    /** Run a function within Hive state (SessionState, HiveConf, Hive client and class loader) */
    def withHiveState[A](spark:SparkSession)(f: => A): A = {
        val catalog = hiveCatalog(spark)
        catalog.client.withHiveState(f)
    }

    /**
     * This methid will create a Hive session without using the Hive classloaders. This method is mainly used
     * to mitigate a bug in CDP 7.1, where Hive is accessed from within the Spark world without class loader
     * isolation.
     * @param spark
     * @param f
     * @tparam A
     * @return
     */
    def withHiveSession[A](spark:SparkSession)(f: => A): A = {
        // Check if the given Spark session is the same as last time. If so, then do not create a new Hive client.
        val currentSparkSession = _currentSparkSession.get()
        if (currentSparkSession ne spark) {
            val currentListener = _currentListener.get()
            val currentHive = _currentHive.get()
            if (currentHive != null) {
                assert(currentSparkSession != null)
                assert(currentListener != null)

                // Remove Spark listener again
                currentSparkSession.sparkContext.removeSparkListener(currentListener)
                _currentListener.remove()

                if (Hive.getThreadLocal eq currentHive) {
                    Hive.closeCurrent()
                }
                currentHive.close(false)
                _currentHive.remove()
            }

            // Create new Hive Session
            val conf = new HiveConf(spark.sparkContext.hadoopConfiguration, classOf[Hive])
            val hive = Hive.get(conf)

            // Release Hive session when Spark Context is destroyed
            val listener = new SparkListener {
                override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
                    if (Hive.getThreadLocal() eq hive) {
                        Hive.closeCurrent()
                    }
                    hive.close(false)
                }
            }
            spark.sparkContext.addSparkListener(listener)

            // Set Hive session
            Hive.set(hive)
            _currentSparkSession.set(spark)
            _currentHive.set(hive)
            _currentListener.set(listener)
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
