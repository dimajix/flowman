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

import java.io.StringReader
import java.net.URL
import java.nio.charset.Charset
import java.util.Properties

import scala.collection.JavaConverters._
import scala.util.matching.Regex

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.io.Resources
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.documentation.Generator
import com.dimajix.flowman.documentation.ProjectDoc
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.hadoop.File


object FileGenerator {
    val textTemplate : URL = Resources.getResource(classOf[FileGenerator], "/com/dimajix/flowman/documentation/text")
    val htmlTemplate : URL = Resources.getResource(classOf[FileGenerator], "/com/dimajix/flowman/documentation/html")
    val defaultTemplate : URL = htmlTemplate
}


case class FileGenerator(
    location:Path,
    template:URL = FileGenerator.defaultTemplate,
    includeRelations:Seq[Regex] = Seq.empty,
    excludeRelations:Seq[Regex] = Seq.empty,
    includeMappings:Seq[Regex] = Seq.empty,
    excludeMappings:Seq[Regex] = Seq.empty,
    includeTargets:Seq[Regex] = Seq.empty,
    excludeTargets:Seq[Regex] = Seq.empty
) extends TemplateGenerator(template, includeRelations, excludeRelations, includeMappings, excludeMappings, includeTargets, excludeTargets) {
    private val logger = LoggerFactory.getLogger(classOf[FileGenerator])

    protected override def generateInternal(context:Context, execution: Execution, documentation: ProjectDoc): Unit = {
        val props = new Properties()
        props.load(new StringReader(loadResource("template.properties")))

        val fs = execution.fs

        val uri = location.toUri
        val outputDir = if (uri.getAuthority == null && uri.getScheme == null)
            fs.local(location)
        else
            fs.file(location)

        // Cleanup any existing output directory
        if (outputDir.isDirectory()) {
            outputDir.list().foreach(_.delete(true))
        }
        else if (outputDir.isFile()) {
            outputDir.isFile()
        }
        outputDir.mkdirs()

        generateProjectFile(context, documentation, outputDir, props.asScala.toMap)
    }

    private def generateProjectFile(context:Context, documentation: ProjectDoc, outputDir:File, properties: Map[String,String]) : Unit= {
        val in = properties.getOrElse("template.project.input", "project.vtl")
        val out = properties("template.project.output")

        val projectDoc = renderProject(context, documentation, in)
        writeFile(outputDir / out, projectDoc)
    }

    private def writeFile(file:File, content:String) : Unit = {
        logger.info(s"Writing documentation file '${file.toString}'")
        val out = file.create(true)
        try {
            // Manually convert string to UTF-8 and use write, since writeUTF apparently would write a BOM
            val bytes = Charset.forName("UTF-8").encode(content)
            out.write(bytes.array(), bytes.arrayOffset(), bytes.limit())
        }
        finally {
            out.close()
        }
    }
}


class FileGeneratorSpec extends TemplateGeneratorSpec {
    @JsonProperty(value="location", required=true) private var location:String = _
    @JsonProperty(value="template", required=false) private var template:String = FileGenerator.defaultTemplate.toString

    override def instantiate(context: Context): Generator = {
        val url = getTemplateUrl(context)
        FileGenerator(
            new Path(context.evaluate(location)),
            url,
            includeRelations = includeRelations.map(context.evaluate).map(_.trim).filter(_.nonEmpty).map(_.r),
            excludeRelations = excludeRelations.map(context.evaluate).map(_.trim).filter(_.nonEmpty).map(_.r),
            includeMappings = includeMappings.map(context.evaluate).map(_.trim).filter(_.nonEmpty).map(_.r),
            excludeMappings = excludeMappings.map(context.evaluate).map(_.trim).filter(_.nonEmpty).map(_.r),
            includeTargets = includeTargets.map(context.evaluate).map(_.trim).filter(_.nonEmpty).map(_.r),
            excludeTargets = excludeTargets.map(context.evaluate).map(_.trim).filter(_.nonEmpty).map(_.r)
        )
    }
}