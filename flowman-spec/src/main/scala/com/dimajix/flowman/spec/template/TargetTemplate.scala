/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

import com.dimajix.common.Trilean
import com.dimajix.flowman.documentation.TargetDoc
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.AbstractInstance
import com.dimajix.flowman.model.BaseTemplate
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.spec.target.TargetSpec


case class TargetTemplate(
    instanceProperties: Template.Properties,
    parameters: Seq[Template.Parameter],
    spec:Prototype[Target]
) extends BaseTemplate[Target] with com.dimajix.flowman.model.TargetTemplate {
    override protected def instantiateInternal(context: Context, props: Target.Properties): Target = {
        spec.instantiate(context, Some(props))
    }
}

class TargetTemplateSpec extends TemplateSpec {
    @JsonProperty(value="template", required=true) private var spec:TargetSpec = _

    override def instantiate(context: Context, properties:Option[Template.Properties] = None): TargetTemplate = {
        TargetTemplate(
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
            "before" : {
              "type" : "array",
              "items" : {
                "type" : "string"
              }
            },
            "after" : {
              "type" : "array",
              "items" : {
                "type" : "string"
              }
            },
            "description" : {
              "type" : "string"
            },
            "documentation" : {
              "$ref" : "#/definitions/TargetDocSpec"
            }
         }
      }
    """
)
class TargetTemplateInstanceSpec extends TargetSpec {
    @JsonIgnore
    private[spec] var args:Map[String,String] = Map()

    @JsonAnySetter
    private def setArg(name:String, value:String) : Unit = {
        args = args.updated(name, value)
    }

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): Target = {
        // get template name from member "kind"
        // Lookup template in context
        val identifier = TemplateIdentifier(kind.stripPrefix("template/"))
        val template = context.getTemplate(identifier).asInstanceOf[TargetTemplate]
        val props = instanceProperties(context, properties)

        // parse args
        val parsedArgs = template.arguments(context.evaluate(args))
        template.instantiate(context, props, parsedArgs)
    }
}
