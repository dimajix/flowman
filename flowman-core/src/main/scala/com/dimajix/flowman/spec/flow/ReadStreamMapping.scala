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
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.util.SchemaUtils


case class ReadStreamMapping (
    instanceProperties:Mapping.Properties,
    relation:RelationIdentifier,
    columns:Map[String,String]
) extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[ReadStreamMapping])

    /**
      * Executes this Transform by reading from the specified source and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[MappingIdentifier,DataFrame]): DataFrame = {
        val fields = this.columns
        val relation = context.getRelation(this.relation)
        val schema = if (fields != null && fields.nonEmpty) SchemaUtils.createSchema(fields.toSeq) else null
        logger.info(s"Reading from streaming relation '${this.relation}'")

        relation.readStream(executor, schema)
    }

    /**
      * Returns the dependencies of this mapping, which are empty for an InputMapping
      *
      * @return
      */
    override def dependencies : Array[MappingIdentifier] = {
        Array()
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(input:Map[MappingIdentifier,StructType]) : StructType = {
        require(input != null)

        val fields = this.columns
        if (fields != null && fields.nonEmpty) {
            StructType.of(SchemaUtils.createSchema(fields.toSeq))
        }
        else {
            val relation = context.getRelation(this.relation)
            StructType(relation.schema.fields)
        }
    }
}



class ReadStreamMappingSpec extends MappingSpec {
    @JsonProperty(value = "relation", required = true) private var relation:String = _
    @JsonProperty(value = "columns", required=false) private var columns:Map[String,String] = Map()

    override def instantiate(context: Context): ReadStreamMapping = {
        val props = instanceProperties(context)
        val relation = RelationIdentifier(context.evaluate(this.relation))
        val columns = this.columns.mapValues(context.evaluate)
        ReadStreamMapping(props, relation, columns)
    }
}
