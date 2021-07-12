package com.dimajix.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.optimizer.PushPredicateThroughNonJoin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule


object PushDownPredicate extends Rule[LogicalPlan] with PredicateHelper {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform PushPredicateThroughNonJoin.applyLocally
}
