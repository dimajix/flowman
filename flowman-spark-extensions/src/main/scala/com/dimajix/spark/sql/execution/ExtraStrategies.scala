/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

import com.dimajix.spark.sql.catalyst.optimizer.CreateEagerCache
import com.dimajix.spark.sql.catalyst.plans.logical.CountRecords
import com.dimajix.spark.sql.catalyst.plans.logical.EagerCache


object ExtraStrategies extends Strategy {
    def register(spark:SparkSession) : Unit = {
        spark.sqlContext.experimental.extraStrategies
            = spark.sqlContext.experimental.extraStrategies ++ Seq(ExtraStrategies)
        spark.sqlContext.experimental.extraOptimizations
            = spark.sqlContext.experimental.extraOptimizations ++ Seq(CreateEagerCache)
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case CountRecords(child, counter) => CountRecordsExec(planLater(child), counter) :: Nil
        case EagerCache(child, caches) => EagerCacheExec(planLater(child), caches.map(planLater)) :: Nil
        case _ => Nil
    }
}
