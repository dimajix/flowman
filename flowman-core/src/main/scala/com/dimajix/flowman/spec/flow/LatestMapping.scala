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
import org.apache.spark.sql.functions.row_number
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType


case class LatestMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    keyColumns:Seq[String],
    versionColumn:String
) extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[LatestMapping])

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

        val window = Window.partitionBy(keyColumns.map(col):_*).orderBy(col(versionColumn).desc)
        val result = df.select(col("*"), row_number().over(window) as "flowman_gen_rank")
            .filter(col("flowman_gen_rank") === 1)
            .drop(col("flowman_gen_rank"))

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
        val result = input(this.input)

        Map("main" -> result)
    }
}


class LatestMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input:String = _
    @JsonProperty(value = "versionColumn", required = true) private var versionColumn:String = _
    @JsonProperty(value = "keyColumns", required = true) private var keyColumns:Seq[String] = Seq()

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): LatestMapping = {
        LatestMapping(
            instanceProperties(context),
            MappingOutputIdentifier(context.evaluate(input)),
            keyColumns.map(context.evaluate),
            context.evaluate(versionColumn)
        )
    }
}
