/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.row_number
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.flow.RankMapping.Earliest
import com.dimajix.flowman.spec.flow.RankMapping.Latest
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
    versionColumn:String,
    mode:RankMapping.Mode,
    filter:Option[String] = None
) extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[RankMapping])

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param tables
      * @return
      */
    override def execute(executor:Executor, tables:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(executor != null)
        require(tables != null)

        logger.info(s"Selecting latest version in '$input' using key columns ${keyColumns.mkString(",")} and version column $versionColumn")

        val df = tables(input)

        val order = mode match {
            case Earliest => col(versionColumn).asc
            case Latest => col(versionColumn).desc
        }
        val window = Window.partitionBy(keyColumns.map(col):_*).orderBy(order)
        val latest = df.select(col("*"), row_number().over(window) as "flowman_gen_rank")
            .filter(col("flowman_gen_rank") === 1)
            .drop(col("flowman_gen_rank"))

        // Apply optional filter to updates (for example for removing DELETEs)
        val result = filter.map(f => latest.where(f)).getOrElse(latest)

        Map("main" -> result)
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @return
      */
    override def inputs : Seq[MappingOutputIdentifier] = {
        Seq(input)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(input != null)
        val result = input(this.input)

        Map("main" -> result)
    }
}


abstract class RankMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input:String = _
    @JsonProperty(value = "versionColumn", required = true) private var versionColumn:String = _
    @JsonProperty(value = "keyColumns", required = true) private var keyColumns:Seq[String] = Seq()
    @JsonProperty(value = "filter", required = false) private var filter: Option[String] = None

    protected def mode : RankMapping.Mode

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context): RankMapping = {
        RankMapping(
            instanceProperties(context),
            MappingOutputIdentifier(context.evaluate(input)),
            keyColumns.map(context.evaluate),
            context.evaluate(versionColumn),
            mode,
            filter.map(context.evaluate).map(_.trim).filter(_.nonEmpty)
        )
    }
}

class LatestMappingSpec extends RankMappingSpec {
    override protected def mode: RankMapping.Mode = RankMapping.Latest
}

class EarliestMappingSpec extends RankMappingSpec {
    override protected def mode: RankMapping.Mode = RankMapping.Earliest
}
