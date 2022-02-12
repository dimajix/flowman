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
    val defaultTemplate : URL = textTemplate
}


case class FileGenerator(
    location:Path,
    template:URL = FileGenerator.defaultTemplate
) extends TemplateGenerator(template) {
    private val logger = LoggerFactory.getLogger(classOf[FileGenerator])

    override def generate(context:Context, execution: Execution, documentation: ProjectDoc): Unit = {
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

        val projectDoc = renderProject(context, documentation)
        writeFile(outputDir / "project.txt", projectDoc)
    }

    private def writeFile(file:File, content:String) : Unit = {
        logger.info(s"Writing documentation file '${file.toString}'")
        val out = file.create(true)
        try {
            // Manually convert string to UTF-8 and use write, since writeUTF apparently would write a BOM
            val bytes = Charset.forName("UTF-8").encode(content)
            val output = file.create(true)
            out.write(bytes.array(), bytes.arrayOffset(), bytes.limit())
        }
        finally {
            out.close()
        }
    }
}


class FileGeneratorSpec extends GeneratorSpec {
    @JsonProperty(value="location", required=true) private var location:String = _
    @JsonProperty(value="template", required=false) private var template:String = FileGenerator.defaultTemplate.toString

    override def instantiate(context: Context): Generator = {
        FileGenerator(
            new Path(context.evaluate(location)),
            new URL(context.evaluate(template))
        )
    }
}
