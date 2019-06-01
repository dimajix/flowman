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
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.model.PartitionedRelation
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.types.ArrayValue
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.RangeValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.util.SchemaUtils


case class ReadRelationMapping(
    instanceProperties:Mapping.Properties,
    relation:RelationIdentifier,
    columns:Map[String,String],
    partitions:Map[String,FieldValue]
) extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[ReadRelationMapping])

    /**
      * Executes this Transform by reading from the specified source and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[MappingOutputIdentifier,DataFrame]): Map[String,DataFrame] = {
        require(executor != null)
        require(input != null)

        val schema = if (columns.nonEmpty) SchemaUtils.createSchema(columns.toSeq) else null
        logger.info(s"Reading from relation '$relation' with partitions ${partitions.map(kv => kv._1 + "=" + kv._2).mkString(",")}")
        val rel = context.getRelation(relation)
        val result = rel.read(executor, schema, partitions)

        Map("default" -> result)
    }

    /**
      * Returns the dependencies of this mapping, which are empty for an ReadRelationMapping
      *
      * @return
      */
    override def dependencies : Seq[MappingOutputIdentifier] = {
        Seq()
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(input != null)

        val result = if (columns.nonEmpty) {
            StructType.of(SchemaUtils.createSchema(columns.toSeq))
        }
        else {
            context.getRelation(relation) match {
                case pt:PartitionedRelation => StructType(pt.schema.fields ++ pt.partitions.map(_.field))
                case relation:Relation => StructType(relation.schema.fields)
            }
        }

        Map("default" -> result)
    }
}



class ReadRelationMappingSpec extends MappingSpec {
    @JsonProperty(value = "relation", required = true) private var relation:String = _
    @JsonProperty(value = "columns", required=false) private var columns:Map[String,String] = Map()
    @JsonProperty(value = "partitions", required=false) private var partitions:Map[String,FieldValue] = Map()

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): ReadRelationMapping = {
        val partitions= this.partitions.mapValues {
                case v: SingleValue => SingleValue(context.evaluate(v.value))
                case v: ArrayValue => ArrayValue(v.values.map(context.evaluate))
                case v: RangeValue => RangeValue(context.evaluate(v.start), context.evaluate(v.end), context.evaluate(v.step))
            }
        ReadRelationMapping(
            instanceProperties(context),
            RelationIdentifier(context.evaluate(relation)),
            context.evaluate(columns),
            partitions
        )
    }
}
