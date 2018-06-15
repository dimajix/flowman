/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.With

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier


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
    override def execute(executor:Executor, input:Map[MappingIdentifier,DataFrame]) : DataFrame = {
        implicit val context = executor.context
        // Register all input DataFrames as temp views
        input.foreach(kv => kv._2.createOrReplaceTempView(kv._1.name))
        // Execute query
        val result = executor.spark.sql(sql)
        // Call SessionCatalog.dropTempView to avoid unpersisting the possibly cached dataset.
        input.foreach(kv => executor.spark.sessionState.catalog.dropTempView(kv._1.name))
        result
    }

    /**
      * Resolves all dependencies required to execute the SQL
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context:Context) : Array[MappingIdentifier] = {
        val plan = CatalystSqlParser.parsePlan(sql)
        resolveDependencies(plan).map(MappingIdentifier.parse).toArray
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
