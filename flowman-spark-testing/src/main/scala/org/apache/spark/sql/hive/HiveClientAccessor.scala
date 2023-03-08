/*
 * Copyright (C) 2021 The Flowman Authors
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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogWithListener


object HiveClientAccessor {
    /** Run a function within Hive state (SessionState, HiveConf, Hive client and class loader) */
    def withHiveState[A](spark:SparkSession)(f: => A): A = {
        val catalog = hiveCatalog(spark)
        catalog.client.withHiveState(f)
    }

    private def hiveCatalog(spark:SparkSession) : HiveExternalCatalog = {
        def unwrap(catalog:ExternalCatalog) : HiveExternalCatalog = catalog match {
            case c: ExternalCatalogWithListener => unwrap(c.unwrapped)
            case h: HiveExternalCatalog => h
        }
        unwrap(spark.sessionState.catalog.externalCatalog)
    }
}
