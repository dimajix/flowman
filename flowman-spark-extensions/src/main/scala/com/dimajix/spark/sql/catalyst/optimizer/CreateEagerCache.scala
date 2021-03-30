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

package com.dimajix.spark.sql.catalyst.optimizer

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec

import com.dimajix.spark.sql.catalyst.plans.logical.EagerCache
import com.dimajix.spark.sql.execution.EagerCacheExec


object CreateEagerCache extends Rule[LogicalPlan] with PredicateHelper {
    def apply(plan: LogicalPlan): LogicalPlan = {
        val cacheCounts = new util.IdentityHashMap[SparkPlan,(InMemoryRelation,Int)]()

        def countSubCaches(relation:InMemoryRelation) : Unit = {
            val cp = SparkShim.getCachedPlan(relation)
            val (_,count) = cacheCounts.getOrDefault(cp, (null,0))
            // Do not increase counter, if it was marked as "already cached"
            if (count >= 0)
                cacheCounts.put(cp, (relation, count+1))

            // Recursively follow any sub-caches below current cache
            cp.foreachUp {
                case tableScan:InMemoryTableScanExec =>
                    countSubCaches(tableScan.relation)
                case EagerCacheExec(_, caches) =>
                    // Mark this plan not to be cached again, since there already is an eager cache
                    caches.foreach(c => cacheCounts.put(c, (null,-1)))
                case _ =>
            }
        }

        def countCaches(plan:LogicalPlan) : Unit = {
            plan.foreachUp {
                case relation:InMemoryRelation =>
                    countSubCaches(relation)
                case EagerCache(_, caches) =>
                    // Mark this plan not to be cached again, since there already is an eager cache
                    caches.foreach(c => cacheCounts.put(SparkShim.getCachedPlan(c), (null,-1)))
                case _ =>
            }
        }

        countCaches(plan)

        // Collect all caches, which are used more than once
        val caches = cacheCounts.asScala
            .filter(_._2._2 > 1)
            .map(_._2._1)

        if (caches.isEmpty) // || plan.collectFirst { case _:EagerCache => true }.exists(identity))
            plan
        else
            EagerCache(plan, caches.toSeq)
    }
}
