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

package com.dimajix.flowman.spec.mapping

import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingTemplate
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.spec.TemplateSpec


class MappingTemplateSpec extends TemplateSpec[Mapping] with MappingTemplate {
    @JsonProperty(value="template", required=true) private var spec:MappingSpec = _

    override def instantiateInternal(context: Context, name: String): Mapping = {
        synchronized {
            spec.name = name
            spec.instantiate(context)
        }
    }
}


class MappingTemplateInstanceSpec extends MappingSpec {
    @JsonIgnore
    private[spec] var args:Map[String,String] = Map()

    @JsonAnySetter
    private def setArg(name:String, value:String) : Unit = {
        args = args.updated(name, value)
    }

    override def instantiate(context: Context): Mapping = {
        // get template name from member "kind"
        // Lookup template in context
        val identifier = TemplateIdentifier(kind.stripPrefix("template/"))
        val template = context.getTemplate(identifier).asInstanceOf[MappingTemplate]

        // parse args
        val parsedArgs = template.arguments(context.evaluate(args))
        template.instantiate(context, name, parsedArgs)
    }
}
