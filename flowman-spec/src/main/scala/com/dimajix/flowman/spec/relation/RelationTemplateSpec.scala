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

package com.dimajix.flowman.spec.relation

import java.util

import scala.collection.JavaConverters._

import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationTemplate
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.spec.TemplateSpec


class RelationTemplateSpec extends TemplateSpec[Relation] with RelationTemplate {
    @JsonProperty(value="template", required=true) private var spec:RelationSpec = _

    override def instantiateInternal(context: Context, name: String): Relation = {
        synchronized {
            spec.name = name
            spec.instantiate(context)
        }
    }
}


class RelationTemplateInstanceSpec extends RelationSpec {
    @JsonAnySetter
    private var args:java.util.Map[String,String] = new util.HashMap[String,String]()

    override def instantiate(context: Context): Relation = {
        // get template name from member "kind"
        // Lookup template in context
        val identifier = TemplateIdentifier(kind.stripPrefix("template/"))
        val template:RelationTemplate = context.getTemplate(identifier).asInstanceOf[RelationTemplate]

        // parse args
        val parsedArgs = template.arguments(args.asScala.toMap)
        template.instantiate(context, name, parsedArgs)
    }
}
