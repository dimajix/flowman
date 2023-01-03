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
import java.nio.charset.Charset
import java.util.Properties

import scala.collection.JavaConverters._
import scala.util.matching.Regex

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.documentation.Generator
import com.dimajix.flowman.documentation.ProjectDoc
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.fs.File


object FileGenerator {
    val textTemplate : File = File.ofResource("com/dimajix/flowman/documentation/text")
    val htmlTemplate : File = File.ofResource( "com/dimajix/flowman/documentation/html")
    val htmlCssTemplate : File = File.ofResource( "com/dimajix/flowman/documentation/html+css")
    val defaultTemplate : File = htmlTemplate
}


case class FileGenerator(
    location:Path,
    template:File = FileGenerator.defaultTemplate,
    includeRelations:Seq[Regex] = Seq(".*".r),
    excludeRelations:Seq[Regex] = Seq.empty,
    includeMappings:Seq[Regex] = Seq(".*".r),
    excludeMappings:Seq[Regex] = Seq.empty,
    includeTargets:Seq[Regex] = Seq(".*".r),
    excludeTargets:Seq[Regex] = Seq.empty
) extends TemplateGenerator(template, includeRelations, excludeRelations, includeMappings, excludeMappings, includeTargets, excludeTargets) {
    private val logger = LoggerFactory.getLogger(classOf[FileGenerator])

    protected override def generateInternal(context:Context, execution: Execution, documentation: ProjectDoc) : Unit = {
        val props = new Properties()
        props.load(new StringReader(loadResource("template.properties")))

        val fs = execution.fs
        val outputDir = fs.file(location)

        // Cleanup any existing output directory
        if (outputDir.isDirectory()) {
            outputDir.list().foreach(_.delete(true))
        }
        else if (outputDir.isFile()) {
            outputDir.delete(false)
        }
        outputDir.mkdirs()

        generateProjectFiles(context, documentation, outputDir, props.asScala.toMap)
    }

    private def generateProjectFiles(context:Context, documentation: ProjectDoc, outputDir:File, properties: Map[String,String]) : Unit = {
        val keyPattern = raw"template\.(.+)\.input".r
        val keys = properties.keySet.collect { case keyPattern(key) => key }
        keys.foreach { key =>
            val in = properties(s"template.$key.input")
            val out = properties(s"template.$key.output")
            val outputFile = outputDir / out

            logger.info(s"Writing documentation template '$key' to file '${outputFile.toString}'")
            val projectDoc = renderTemplate(context, documentation, in)
            writeFile(outputFile, projectDoc)
        }
    }

    private def writeFile(file:File, content:String) : Unit = {
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

    override def instantiate(context: Context, properties:Option[Generator.Properties]): Generator = {
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
