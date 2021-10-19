package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.QueryPlan


/**
 * A logical node that can be used for a command that requires its children to be only analyzed,
 * but not optimized.
 */
trait AnalysisOnlyCommand extends Command {
    val isAnalyzed: Boolean
    def childrenToAnalyze: Seq[LogicalPlan]
    override final def children: Seq[LogicalPlan] = if (isAnalyzed) Nil else childrenToAnalyze
    override def innerChildren: Seq[QueryPlan[_]] = if (isAnalyzed) childrenToAnalyze else Nil
    def markAsAnalyzed(): LogicalPlan
}
