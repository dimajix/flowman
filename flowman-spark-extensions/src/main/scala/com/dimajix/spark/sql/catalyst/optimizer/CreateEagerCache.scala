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

        def addCache(relation:InMemoryRelation) : Unit = {
            val cp = relation.cachedPlan
            val (rel,count) = cacheCounts.getOrDefault(cp, (null,0))
            if (count >= 0)
                cacheCounts.put(cp, (relation, count + 1))
        }
        def disallowCache(relation:InMemoryRelation) : Unit = {
            val cp = relation.cachedPlan
            cacheCounts.put(cp, (relation, -1))
        }

        def countSubCaches(relation:InMemoryRelation) : Unit = {
            addCache(relation)

            // Recursively follow any sub-caches below current cache
            val cp = relation.cachedPlan
            cp.foreachUp {
                case tableScan:InMemoryTableScanExec =>
                    countSubCaches(tableScan.relation)
                case EagerCacheExec(_, caches) =>
                    // Mark this plan not to be cached again, since there already is an eager cache
                    caches.foreach { c =>
                        c.collectFirst { case ex:InMemoryTableScanExec => ex }
                            .foreach(scan => disallowCache(scan.relation))
                    }
                case _ =>
            }
        }

        def countCaches(plan:LogicalPlan) : Unit = {
            plan.foreachUp {
                case relation:InMemoryRelation =>
                    countSubCaches(relation)
                case EagerCache(_, caches) =>
                    // Mark this plan not to be cached again, since there already is an eager cache
                    caches.foreach(c => disallowCache(c))
                case _ =>
            }
        }

        // Skip top level EagerCache, which might be replaced later and therefore shouldn't be counted
        countCaches(plan)

        // Collect all caches, which are used more than once
        val caches = cacheCounts.asScala
            .filter(_._2._2 > 1)
            .map(_._2._1)
            .toSeq

        // Now check what we need to do
        if (caches.nonEmpty)
            EagerCache(plan, caches)
        else
            plan
    }
}
