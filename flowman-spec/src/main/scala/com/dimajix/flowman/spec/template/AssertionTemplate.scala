/*
 * Copyright 2021 Kaya Kupferschmidt
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
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.spec.assertion.AssertionSpec


case class AssertionTemplate(
    instanceProperties: Template.Properties,
    parameters: Seq[Template.Parameter],
    spec:AssertionSpec
) extends BaseTemplate[Assertion] with com.dimajix.flowman.model.AssertionTemplate {
    override protected def instantiateInternal(context: Context, name: String): Assertion = {
        synchronized {
            spec.name = name
            spec.instantiate(context)
        }
    }
}

class AssertionTemplateSpec extends TemplateSpec {
    @JsonProperty(value="template", required=true) private var spec:AssertionSpec = _

    override def instantiate(context: Context): AssertionTemplate = {
        AssertionTemplate(
            instanceProperties(context),
            parameters.map(_.instantiate(context)),
            spec
        )
    }
}


class AssertionTemplateInstanceSpec extends AssertionSpec {
    @JsonIgnore
    private[spec] var args: Map[String, String] = Map()

    @JsonAnySetter
    private def setArg(name: String, value: String): Unit = {
        args = args.updated(name, value)
    }

    override def instantiate(context: Context): Assertion = {
        // get template name from member "kind"
        // Lookup template in context
        val identifier = TemplateIdentifier(kind.stripPrefix("template/"))
        val template = context.getTemplate(identifier).asInstanceOf[AssertionTemplate]

        // parse args
        val parsedArgs = template.arguments(context.evaluate(args))
        template.instantiate(context, name, parsedArgs)
    }
}
