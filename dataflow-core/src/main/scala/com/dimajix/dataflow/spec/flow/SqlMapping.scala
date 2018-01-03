package com.dimajix.dataflow.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.With

import com.dimajix.dataflow.execution.Context

class SqlMapping extends BaseMapping {
    @JsonProperty("sql") private[spec] var _sql:String = _
    @JsonProperty("file") private[spec] var _file:String = _

    def sql(implicit context: Context) : String = context.evaluate(_sql)
    def file(implicit context: Context) : String = context.evaluate(_file)

    override def execute(implicit context:Context) = {
        context.session.sql(sql)
    }
    override def dependencies(implicit context:Context) : Array[String] = {
        val plan = CatalystSqlParser.parsePlan(sql)
        resolveDependencies(plan).toArray
    }

    private def resolveDependencies(plan:LogicalPlan) : Seq[String] = {
        val cteNames = plan
            .collect { case p:With => p.cteRelations.map(kv => kv._1)}
            .flatten
            .toSet
        val cteDependencies = plan
            .collect { case p: With =>
                p.cteRelations
                    .map(kv => kv._2.child)
                    .flatMap(resolveDependencies)
                    .filter(!cteNames.contains(_))
            }
            .flatten
            .toSet
        val tables = plan.collect { case p:UnresolvedRelation if !cteNames.contains(p.tableName) => p.tableName }.toArray
        tables ++ cteDependencies
    }
}
