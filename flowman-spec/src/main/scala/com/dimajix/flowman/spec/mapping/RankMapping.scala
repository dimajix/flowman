/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.mapping

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.row_number

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.mapping.RankMapping.Earliest
import com.dimajix.flowman.spec.mapping.RankMapping.Latest
import com.dimajix.flowman.types.StructType


object RankMapping {
    sealed abstract class Mode {}
    case object Earliest extends Mode
    case object Latest extends Mode

    def mode(m:String) : Mode = {
        m.toLowerCase(Locale.ROOT) match {
            case "earliest" => Earliest
            case "latest" => Latest
            case _ => throw new IllegalArgumentException(s"Ranking mode '$m' not supported, must bei either 'earliest' or 'latest'")
        }
    }
}

case class RankMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    keyColumns:Seq[String],
    versionColumns:Seq[String],
    mode:RankMapping.Mode,
    filter:Option[String] = None
) extends BaseMapping {
    /**
     * Returns the dependencies of this mapping, which is exactly one input table
     *
     * @return
     */
    override def inputs : Set[MappingOutputIdentifier] = {
        Set(input) ++ expressionDependencies(filter)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param execution
      * @param tables
      * @return
      */
    override def execute(execution:Execution, tables:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(execution != null)
        require(tables != null)

        val df = tables(input)

        val order = mode match {
            case Earliest => versionColumns.map(col(_).asc)
            case Latest => versionColumns.map(col(_).desc)
        }
        val window = Window.partitionBy(keyColumns.map(col):_*).orderBy(order:_*)
        val latest = df.select(col("*"), row_number().over(window) as "flowman_gen_rank")
            .filter(col("flowman_gen_rank") === 1)
            .drop(col("flowman_gen_rank"))

        // Apply optional filter to updates (for example for removing DELETEs)
        val result = applyFilter(latest, filter, tables)

        Map("main" -> result)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(execution:Execution, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(execution != null)
        require(input != null)

        val result = input(this.input)

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
    }
}


abstract class RankMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input:String = _
    @JsonProperty(value = "versionColumns", required = true) private var versionColumns:Seq[String] = Seq()
    @JsonProperty(value = "keyColumns", required = true) private var keyColumns:Seq[String] = Seq()
    @JsonProperty(value = "filter", required = false) private var filter: Option[String] = None

    protected def mode : RankMapping.Mode

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): RankMapping = {
        RankMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier(context.evaluate(input)),
            keyColumns.map(context.evaluate),
            versionColumns.map(context.evaluate),
            mode,
            context.evaluate(filter)
        )
    }
}

class LatestMappingSpec extends RankMappingSpec {
    override protected def mode: RankMapping.Mode = RankMapping.Latest
}

class EarliestMappingSpec extends RankMappingSpec {
    override protected def mode: RankMapping.Mode = RankMapping.Earliest
}
