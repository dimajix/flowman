/*
 * Copyright 2022 Kaya Kupferschmidt
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

import org.apache.spark.sql.util.QueryExecutionListener
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.testing.LocalSparkSession


class SessionCleanerTest extends AnyFlatSpec with Matchers with LocalSparkSession with MockFactory {
    "The SessionCleaner" should "deregister QueryExecutionListeners" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()

        val listener = mock[QueryExecutionListener]
        session.spark.listenerManager.register(listener)

        // Execute a query
        (listener.onSuccess _).expects(*,*,*)
        spark.sql("SELECT 1").count()

        {
            val owner = new Object
            session.cleaner.registerQueryExecutionListener(owner, listener)

            // Execute a query, such that the listener is invoked
            (listener.onSuccess _).expects(*,*,*)
            spark.sql("SELECT 1").count()
        }

        // Force garbage collection
        System.gc()

        // Execute a query - the listener should now be removed
        spark.sql("SELECT 1").count()

        session.shutdown()
    }
}
