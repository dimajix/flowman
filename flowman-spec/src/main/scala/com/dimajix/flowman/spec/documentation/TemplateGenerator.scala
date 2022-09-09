/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.documentation

import java.net.URL
import java.nio.charset.Charset

import scala.util.matching.Regex

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.io.Resources

import com.dimajix.flowman.documentation.AbstractGenerator
import com.dimajix.flowman.documentation.MappingDoc
import com.dimajix.flowman.documentation.MappingDocWrapper
import com.dimajix.flowman.documentation.ProjectDoc
import com.dimajix.flowman.documentation.ProjectDocWrapper
import com.dimajix.flowman.documentation.RelationDoc
import com.dimajix.flowman.documentation.RelationDocWrapper
import com.dimajix.flowman.documentation.TargetDoc
import com.dimajix.flowman.documentation.TargetDocWrapper
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.Identifier


abstract class TemplateGenerator(
    template:URL,
    includeRelations:Seq[Regex] = Seq(".*".r),
    excludeRelations:Seq[Regex] = Seq.empty,
    includeMappings:Seq[Regex] = Seq(".*".r),
    excludeMappings:Seq[Regex] = Seq.empty,
    includeTargets:Seq[Regex] = Seq(".*".r),
    excludeTargets:Seq[Regex] = Seq.empty
) extends AbstractGenerator {
    override def generate(context:Context, execution: Execution, documentation: ProjectDoc): Unit = {
        def checkRegex(id:Identifier[_], regex:Regex) : Boolean = {
            regex.unapplySeq(id.toString).nonEmpty || regex.unapplySeq(id.name).nonEmpty
        }
        // Apply all filters
        val relations = documentation.relations.filter { relation =>
            includeRelations.exists(regex => checkRegex(relation.identifier, regex)) &&
                !excludeRelations.exists(regex => checkRegex(relation.identifier, regex))
        }
        val mappings = documentation.mappings.filter { mapping =>
            includeMappings.exists(regex => checkRegex(mapping.identifier, regex)) &&
                !excludeMappings.exists(regex => checkRegex(mapping.identifier, regex))
        }
        val targets = documentation.targets.filter { target =>
            includeTargets.exists(regex => checkRegex(target.identifier, regex)) &&
                !excludeTargets.exists(regex => checkRegex(target.identifier, regex))
        }
        val doc = documentation.copy(
            relations = relations,
            mappings = mappings,
            targets = targets
        )

        generateInternal(context:Context, execution: Execution, doc)
    }

    protected def generateInternal(context:Context, execution: Execution, documentation: ProjectDoc): Unit

    protected def renderTemplate(context:Context, documentation: ProjectDoc, template:String="project.vtl") : String = {
        val temp = loadResource(template)
        context.evaluate(temp, Map("project" -> ProjectDocWrapper(documentation)))
    }

    protected def loadResource(name: String): String = {
        val path = template.getPath
        val url =
            if (path.endsWith("/"))
                new URL(template.toString + name)
            else
                new URL(template.toString + "/" + name)
        Resources.toString(url, Charset.forName("UTF-8"))
    }
}


abstract class TemplateGeneratorSpec extends GeneratorSpec {
    @JsonProperty(value="template", required=false) private var template:String = FileGenerator.defaultTemplate.toString
    @JsonProperty(value="includeRelations", required=false) protected var includeRelations:Seq[String] = Seq(".*")
    @JsonProperty(value="excludeRelations", required=false) protected var excludeRelations:Seq[String] = Seq.empty
    @JsonProperty(value="includeMappings", required=false) protected var includeMappings:Seq[String] = Seq(".*")
    @JsonProperty(value="excludeMappings", required=false) protected var excludeMappings:Seq[String] = Seq.empty
    @JsonProperty(value="includeTargets", required=false) protected var includeTargets:Seq[String] = Seq(".*")
    @JsonProperty(value="excludeTargets", required=false) protected var excludeTargets:Seq[String] = Seq.empty

    protected def getTemplateUrl(context: Context): URL = {
        context.evaluate(template) match {
            case "text" => FileGenerator.textTemplate
            case "html" => FileGenerator.htmlTemplate
            case "html+css" => FileGenerator.htmlCssTemplate
            case str => new URL(str)
        }
    }
}
