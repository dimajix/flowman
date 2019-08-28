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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lead
import org.slf4j.LoggerFactory

import com.dimajix.common.MapIgnoreCase
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType

case class HistorizeMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    keyColumns:Seq[String],
    timeColumn:String,
    validFromColumn:String,
    validToColumn:String
) extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[HistorizeMapping])

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

        logger.info(s"Creating history for '$input' using key columns ${keyColumns.mkString(",")} and time column $timeColumn")

        val df = tables(input)

        val window = Window.partitionBy(keyColumns.map(col):_*)
            .orderBy(col(timeColumn))
            .rowsBetween(1,1)
        val result = df.select(
            col("*"),
            col(timeColumn) as validFromColumn,
            lead(col(timeColumn), 1).over(window) as validToColumn)

        Map("main" -> result)
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @return
      */
    override def dependencies : Seq[MappingOutputIdentifier] = {
        Seq(input)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(input != null)

        val fields = input(this.input).fields
        val fieldsByName = MapIgnoreCase(fields.map(f => (f.name, f)).toMap)
        val result = StructType(
            fields
            :+ fieldsByName(timeColumn).copy(name=validFromColumn, description = None)
            :+ fieldsByName(timeColumn).copy(name=validToColumn, description = None, nullable = true)
        )

        Map("main" -> result)
    }
}



class HistorizeMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input:String = _
    @JsonProperty(value = "keyColumns", required = true) private var keyColumns:Seq[String] = Seq()
    @JsonProperty(value = "timeColumn", required = true) private var versionColumn:String = _
    @JsonProperty(value = "validFromColumn", required = false) private var validFromColumn:String = "valid_from"
    @JsonProperty(value = "validToColumn", required = false) private var validToColumn:String = "valid_to"

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): HistorizeMapping = {
        HistorizeMapping(
            instanceProperties(context),
            MappingOutputIdentifier(context.evaluate(input)),
            keyColumns.map(context.evaluate),
            context.evaluate(versionColumn),
            context.evaluate(validFromColumn),
            context.evaluate(validToColumn)
        )
    }
}
