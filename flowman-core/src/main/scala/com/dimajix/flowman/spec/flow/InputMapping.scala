/*
 * Copyright 2018 Kaya Kupferschmidt
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
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.TableIdentifier
import com.dimajix.flowman.spec.schema.ArrayValue
import com.dimajix.flowman.spec.schema.FieldValue
import com.dimajix.flowman.spec.schema.RangeValue
import com.dimajix.flowman.spec.schema.SingleValue
import com.dimajix.flowman.util.SchemaUtils


class InputMapping extends BaseMapping {
    @JsonProperty(value = "source", required = true) private var _source:String = _
    @JsonProperty(value = "columns", required=false) private var _columns:Map[String,String] = _
    @JsonProperty(value = "partitions", required=false) private var _partitions:Map[String,FieldValue] = _

    def source(implicit context:Context) : RelationIdentifier = RelationIdentifier.parse(context.evaluate(_source))
    def columns(implicit context:Context) : Map[String,String] = if (_columns != null) _columns.mapValues(context.evaluate) else null
    def partitions(implicit context:Context) : Map[String,FieldValue] = {
        if (_partitions != null) {
            _partitions.mapValues {
                case v: SingleValue => SingleValue(context.evaluate(v.value))
                case v: ArrayValue => ArrayValue(v.values.map(context.evaluate))
                case v: RangeValue => RangeValue(context.evaluate(v.start), context.evaluate(v.end))
            }
        }
        else {
            Map.empty[String,FieldValue]
        }
    }

    /**
      * Executes this Transform by reading from the specified source and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]): DataFrame = {
        implicit val context = executor.context
        val relation = context.getRelation(source)
        val fields = columns
        val schema = if (fields != null) SchemaUtils.createSchema(fields.toSeq) else null
        relation.read(executor, schema, partitions)
    }

    /**
      * Returns the dependencies of this mapping, which are empty for an InputMapping
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context:Context) : Array[TableIdentifier] = {
        Array()
    }
}
