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

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.transforms.ExplodeTransformer
import com.dimajix.flowman.transforms.LiftTransformer
import com.dimajix.flowman.transforms.schema.ArrayNode
import com.dimajix.flowman.transforms.schema.Node
import com.dimajix.flowman.transforms.schema.NodeOps
import com.dimajix.flowman.transforms.schema.Path
import com.dimajix.flowman.types.StructType


object ExplodeMapping {
    case class Columns(
        keep: Seq[Path],
        drop: Seq[Path],
        rename: Map[String,Path]
    )
}

case class ExplodeMapping(
    instanceProperties : Mapping.Properties,
    input : MappingOutputIdentifier,
    array: Path,
    outerColumns: ExplodeMapping.Columns,
    innerColumns: ExplodeMapping.Columns,
    flatten: Boolean,
    naming: String
) extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[ExplodeMapping])

    override def outputs: Seq[String] = Seq("main", "explode")

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def dependencies: Seq[MappingOutputIdentifier] =  {
        Seq(input)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param deps
      * @return
      */
    override def execute(executor: Executor, deps: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = {
        require(executor != null)
        require(deps != null)

        logger.info(s"Reassembling input mapping '$input'")

        val in = deps(input)
        val exploded = explode.transform(in)
        val result = lift.transform(exploded)

        Map("main" -> result, "explode" -> exploded)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param deps
      * @return
      */
    override def describe(deps:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(deps != null)

        val in = deps(input)
        val exploded = explode.transform(in)
        val result = lift.transform(exploded)

        Map("main" -> result, "explode" -> exploded)
    }

    private def explode = ExplodeTransformer(array, outerColumns.keep, outerColumns.drop, outerColumns.rename)
    private def lift = LiftTransformer(array.last, innerColumns.keep, innerColumns.drop, innerColumns.rename)
}
