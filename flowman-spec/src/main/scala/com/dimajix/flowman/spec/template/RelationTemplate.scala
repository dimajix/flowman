/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.flowman.spec.template

import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.apache.spark.sql.DataFrame

import com.dimajix.common.Trilean
import com.dimajix.flowman.documentation.RelationDoc
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.AbstractInstance
import com.dimajix.flowman.model.BaseTemplate
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.spec.relation.RelationSpec
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StructType


case class RelationTemplate(
    instanceProperties: Template.Properties,
    parameters: Seq[Template.Parameter],
    spec:Prototype[Relation]
) extends BaseTemplate[Relation] with com.dimajix.flowman.model.RelationTemplate {
    override protected def instantiateInternal(context: Context, props: Relation.Properties): Relation = {
        spec.instantiate(context, Some(props))
    }
}

class RelationTemplateSpec extends TemplateSpec {
    @JsonProperty(value="template", required=true) private var spec:RelationSpec = _

    override def instantiate(context: Context, properties:Option[Template.Properties] = None): RelationTemplate = {
        RelationTemplate(
            instanceProperties(context, properties),
            parameters.map(_.instantiate(context)),
            spec
        )
    }
}


@JsonSchemaInject(
  merge = false,
  json = """
      {
         "additionalProperties" : { "type": "string" },
         "properties" : {
            "kind" : {
               "type" : "string",
               "pattern": "template/.+"
            },
            "metadata" : {
              "$ref" : "#/definitions/MetadataSpec"
            },
            "description" : {
              "type" : "string"
            },
            "documentation" : {
              "$ref" : "#/definitions/RelationDocSpec"
            }
         }
      }
    """
)
class RelationTemplateInstanceSpec extends RelationSpec {
    @JsonIgnore
    private[spec] var args:Map[String,String] = Map()

    @JsonAnySetter
    private def setArg(name:String, value:String) : Unit = {
        args = args.updated(name, value)
    }

    override def instantiate(context: Context, properties:Option[Relation.Properties] = None): Relation = {
        // get template name from member "kind"
        // Lookup template in context
        val identifier = TemplateIdentifier(kind.stripPrefix("template/"))
        val template = context.getTemplate(identifier).asInstanceOf[RelationTemplate]
        val props = instanceProperties(context, properties)

        // parse args
        val parsedArgs = template.arguments(context.evaluate(args))
        template.instantiate(context, props, parsedArgs)
    }
}
