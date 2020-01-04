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
    validToColumn:String,
    filter:Option[String] = None,
    columnInsertPosition:InsertPosition = InsertPosition.END
) extends BaseMapping {
    /**
     * Returns the dependencies of this mapping, which is exactly one input table
     *
     * @return
     */
    override def inputs : Seq[MappingOutputIdentifier] = {
        Seq(input)
    }

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

        val df = tables(input)

        val window = Window.partitionBy(keyColumns.map(col):_*)
            .orderBy(col(timeColumn))
            .rowsBetween(1,1)
        val validFromColumn = col(timeColumn) as this.validFromColumn
        val validToColumn = lead(col(timeColumn), 1).over(window) as this.validToColumn
        val history = columnInsertPosition match {
            case InsertPosition.BEGINNING =>
                df.select(
                    validFromColumn,
                    validToColumn,
                    col("*")
                )
            case InsertPosition.END =>
                df.select(
                    col("*"),
                    validFromColumn,
                    validToColumn
                )
        }

        // Apply optional filter to updates (for example for removing DELETEs)
        val result = filter.map(f => history.where(f)).getOrElse(history)

        Map("main" -> result)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(executor:Executor, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(executor != null)
        require(input != null)

        val fields = input(this.input).fields
        val fieldsByName = MapIgnoreCase(fields.map(f => (f.name, f)).toMap)
        val validFromColumn = fieldsByName(timeColumn).copy(name=this.validFromColumn, description = None)
        val validToColumn = fieldsByName(timeColumn).copy(name=this.validToColumn, description = None, nullable = true)
        val result = columnInsertPosition match {
            case InsertPosition.BEGINNING =>
                StructType(
                    validFromColumn +:
                    validToColumn +:
                    fields
                )
            case InsertPosition.END =>
                StructType(
                    fields :+
                        validFromColumn :+
                        validToColumn
                )
        }

        Map("main" -> result)
    }
}



class HistorizeMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input:String = _
    @JsonProperty(value = "keyColumns", required = true) private var keyColumns:Seq[String] = Seq()
    @JsonProperty(value = "timeColumn", required = true) private var versionColumn:String = _
    @JsonProperty(value = "validFromColumn", required = false) private var validFromColumn:String = "valid_from"
    @JsonProperty(value = "validToColumn", required = false) private var validToColumn:String = "valid_to"
    @JsonProperty(value = "filter", required = false) private var filter: Option[String] = None
    @JsonProperty(value = "columnInsertPosition", required = false) private var columnInsertPosition:String = "end"

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
            context.evaluate(validToColumn),
            context.evaluate(filter),
            InsertPosition.ofString(context.evaluate(columnInsertPosition))
        )
    }
}
