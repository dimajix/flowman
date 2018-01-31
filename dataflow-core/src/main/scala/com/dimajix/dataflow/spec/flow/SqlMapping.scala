package com.dimajix.dataflow.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.With

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.TableIdentifier


class SqlMapping extends BaseMapping {
    @JsonProperty("sql") private[spec] var _sql:String = _
    @JsonProperty("file") private[spec] var _file:String = _

    def sql(implicit context: Context) : String = context.evaluate(_sql)
    def file(implicit context: Context) : String = context.evaluate(_file)

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) = {
        implicit val context = executor.context
        executor.spark.sql(sql)
    }
    override def dependencies(implicit context:Context) : Array[TableIdentifier] = {
        val plan = CatalystSqlParser.parsePlan(sql)
        resolveDependencies(plan).map(TableIdentifier.parse).toArray
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
