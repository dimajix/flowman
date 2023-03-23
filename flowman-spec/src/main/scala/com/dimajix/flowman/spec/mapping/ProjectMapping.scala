/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman.spec.mapping

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.node.JsonNodeType
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.transforms.ProjectTransformer
import com.dimajix.flowman.transforms.schema.Path
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StructType


final case class ProjectMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    columns:Seq[ProjectTransformer.Column],
    filter:Option[String] = None
)
extends BaseMapping {
    /**
     * Returns the dependencies of this mapping, which is exactly one input table
     *
     * @return
     */
    override def inputs : Set[MappingOutputIdentifier] = {
        Set(input) ++ expressionDependencies(filter)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param execution
      * @param tables
      * @return
      */
    override def execute(execution:Execution, tables:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(execution != null)
        require(tables != null)

        val df = tables(input)
        val result = xfs.transform(df)

        // Apply optional filter
        val filteredResult = applyFilter(result, filter, tables)

        Map("main" -> filteredResult)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(execution:Execution, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(execution != null)
        require(input != null)

        val schema = input(this.input)
        val result = xfs.transform(schema)

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
    }

    private def xfs : ProjectTransformer = ProjectTransformer(columns)
}


object ProjectMappingSpec {
    object Column {
        def apply(column:String) : Column = {
            val result = new Column
            result.column = column
            result
        }
    }
    class Column {
        @JsonProperty(value = "column", required = true) private var column:String = _
        @JsonProperty(value = "name", required = false) private var name:Option[String] = None
        @JsonProperty(value = "type", required = false) private var dtype:Option[FieldType] = None
        @JsonProperty(value = "description", required = false) private var description:Option[String] = None

        def instantiate(context: Context) : ProjectTransformer.Column = {
            ProjectTransformer.Column(
                Path(context.evaluate(column)),
                context.evaluate(name),
                dtype,
                context.evaluate(description)
            )
        }
    }

    private class ColumnDeserializer(vc:Class[_]) extends StdDeserializer[Column](vc) {
        import java.io.IOException

        def this() = this(null)

        @throws[IOException]
        @throws[JsonProcessingException]
        def deserialize(jp: JsonParser, ctxt: DeserializationContext): Column = {
            val node = jp.getCodec.readTree[JsonNode](jp)
            node.getNodeType match {
                case JsonNodeType.STRING => {
                    Column(node.asText)
                }
                case JsonNodeType.OBJECT => {
                    jp.getCodec.treeToValue(node, classOf[Column])
                }
                case _ => throw JsonMappingException.from(jp, "Wrong type for value/range")
            }
        }
    }
}
class ProjectMappingSpec extends MappingSpec {
    import ProjectMappingSpec.Column
    import ProjectMappingSpec.ColumnDeserializer

    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonDeserialize(contentUsing=classOf[ColumnDeserializer])
    @JsonProperty(value = "columns", required = true) private var columns: Seq[Column] = Seq()
    @JsonProperty(value = "filter", required=false) private var filter: Option[String] = None

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): ProjectMapping = {
        ProjectMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier(context.evaluate(input)),
            columns.map(_.instantiate(context)),
            context.evaluate(filter)
        )
    }
}
