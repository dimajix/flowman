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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.BaseTemplate
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.spec.schema.SchemaSpec


case class SchemaTemplate(
    instanceProperties: Template.Properties,
    parameters: Seq[Template.Parameter],
    spec:Prototype[Schema]
) extends BaseTemplate[Schema] with com.dimajix.flowman.model.SchemaTemplate {
    override protected def instantiateInternal(context: Context, props: Schema.Properties): Schema = {
        spec.instantiate(context, Some(props))
    }
}

class SchemaTemplateSpec extends TemplateSpec {
    @JsonProperty(value="template", required=true) private var spec:SchemaSpec = _

    override def instantiate(context: Context, properties:Option[Template.Properties] = None): SchemaTemplate = {
        SchemaTemplate(
            instanceProperties(context, properties),
            parameters.map(_.instantiate(context)),
            spec
        )
    }
}


class SchemaTemplateInstanceSpec extends SchemaSpec {
    @JsonIgnore
    private[spec] var args: Map[String, String] = Map()

    @JsonAnySetter
    private def setArg(name: String, value: String): Unit = {
        args = args.updated(name, value)
    }

    override def instantiate(context: Context, properties:Option[Schema.Properties] = None): Schema = {
        // get template name from member "kind"
        // Lookup template in context
        val identifier = TemplateIdentifier(kind.stripPrefix("template/"))
        val template = context.getTemplate(identifier).asInstanceOf[SchemaTemplate]
        val props = instanceProperties(context, "")

        // parse args
        val parsedArgs = template.arguments(context.evaluate(args))
        template.instantiate(context, props, parsedArgs)
    }
}
