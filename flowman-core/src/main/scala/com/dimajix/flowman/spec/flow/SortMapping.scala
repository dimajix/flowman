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
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType


case class SortMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    columns:Seq[(String,String)]
) extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[SortMapping])

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

        logger.info(s"Sorting mapping '$input' by columns ${columns.mkString(",")}")

        val df = tables(input)
        val cols = columns.map(nv =>
            if (nv._2.toLowerCase == "desc")
                col(nv._1).desc
            else
                col(nv._1).asc
        )
        val result = df.sort(cols:_*)

        Map("default" -> result)
    }

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
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

        Map("default" -> result)
    }
}



class SortMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "columns", required = true) private var columns:Seq[Map[String,String]] = Seq()

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): SortMapping = {
        SortMapping(
            instanceProperties(context),
            MappingOutputIdentifier(context.evaluate(input)),
            columns.flatMap(_.mapValues(context.evaluate))
        )
    }
}
