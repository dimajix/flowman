/*
 * Copyright 2019 Kaya Kupferschmidt
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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.transforms.FlattenTransformer
import com.dimajix.flowman.types.StructType


object FlattenMapping {
    def apply(input:String, caseFormat:String) : FlattenMapping  = {
        val result = new FlattenMapping
        result._input = input
        result._naming = caseFormat
        result
    }
}


class FlattenMapping extends BaseMapping {
    @JsonProperty(value = "input", required = true) private[spec] var _input:String = _
    @JsonProperty(value = "naming", required = false) private[spec] var _naming:String = _

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def naming(implicit context: Context) : String = context.evaluate(_naming)

    /**
      * Executes the mapping operation and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor: Executor, input: Map[MappingIdentifier, DataFrame]): DataFrame = {
        require(executor != null)
        require(input != null)

        implicit val icontext = executor.context
        val mappingId = this.input
        val df = input(mappingId)
        val xfs = FlattenTransformer(naming)

        xfs.transform(df)
    }

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context): Array[MappingIdentifier] = Array(input)

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param context
      * @param input
      * @return
      */
    override def describe(context:Context, input:Map[MappingIdentifier,StructType]) : StructType = {
        require(context != null)
        require(input != null)

        implicit val icontext = context
        val mappingId = this.input
        val schema = input(mappingId)
        val xfs = FlattenTransformer(naming)

        xfs.transform(schema)
    }

}
