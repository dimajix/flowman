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
import com.dimajix.flowman.model.Measure
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.spec.measure.MeasureSpec


case class MeasureTemplate(
    instanceProperties: Template.Properties,
    parameters: Seq[Template.Parameter],
    spec:MeasureSpec
) extends BaseTemplate[Measure] with com.dimajix.flowman.model.MeasureTemplate {
    override protected def instantiateInternal(context: Context, name: String): Measure = {
        synchronized {
            spec.name = name
            spec.instantiate(context)
        }
    }
}

class MeasureTemplateSpec extends TemplateSpec {
    @JsonProperty(value="template", required=true) private var spec:MeasureSpec = _

    override def instantiate(context: Context): MeasureTemplate = {
        MeasureTemplate(
            instanceProperties(context),
            parameters.map(_.instantiate(context)),
            spec
        )
    }
}


class MeasureTemplateInstanceSpec extends MeasureSpec {
    @JsonIgnore
    private[spec] var args: Map[String, String] = Map()

    @JsonAnySetter
    private def setArg(name: String, value: String): Unit = {
        args = args.updated(name, value)
    }

    override def instantiate(context: Context): Measure = {
        // get template name from member "kind"
        // Lookup template in context
        val identifier = TemplateIdentifier(kind.stripPrefix("template/"))
        val template = context.getTemplate(identifier).asInstanceOf[MeasureTemplate]

        // parse args
        val parsedArgs = template.arguments(context.evaluate(args))
        template.instantiate(context, name, parsedArgs)
    }
}
