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

package com.dimajix.flowman.tools.schema

import java.io.File
import java.io.FileOutputStream
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{ObjectMapper => JacksonMapper}
import org.kohsuke.args4j.CmdLineException

import com.dimajix.flowman.FLOWMAN_VERSION
import com.dimajix.flowman.JAVA_VERSION
import com.dimajix.flowman.Tool
import com.dimajix.flowman.common.Logging
import com.dimajix.flowman.common.ToolConfig
import com.dimajix.flowman.spec.ModuleSpec
import com.dimajix.flowman.spec.NamespaceSpec
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.ProjectSpec
import com.dimajix.flowman.spec.documentation.DocumenterSpec
import com.dimajix.flowman.tools.schema.impl.MyJsonSchemaGenerator
import com.dimajix.spark.SPARK_VERSION


object Driver {
    def main(args: Array[String]) : Unit = {
        Logging.init()

        Try {
            run(args:_*)
        }
        match {
            case Success (_) =>
                System.exit(0)
            case Failure(ex:CmdLineException) =>
                System.err.println(ex.getMessage)
                ex.getParser.printUsage(System.err)
                System.err.println
                System.exit(1)
            case Failure(exception) =>
                exception.printStackTrace(System.err)
                System.exit(1)
        }
    }

    def run(args: String*) : Unit = {
        val options = new Arguments(args.toArray)
        // Check if only help or version is requested
        if (options.version) {
            println(s"Flowman $FLOWMAN_VERSION")
            println(s"Flowman home: ${ToolConfig.homeDirectory.getOrElse("<unknown>")}")
            println(s"Spark version $SPARK_VERSION")
            println(s"Java version $JAVA_VERSION")
        }
        else if (options.help) {
            options.printHelp(System.out)
        }
        else {
            val driver = new Driver(options)
            driver.run()
        }
    }
}

class Driver(args:Arguments) extends Tool {
    def run() : Unit = {
        val baseDir = Paths.get(args.output)
        Files.createDirectories(baseDir)

        val objectMapper = ObjectMapper.mapper
        val jsonSchemaGenerator = new MyJsonSchemaGenerator(objectMapper)

        val moduleSchema = jsonSchemaGenerator.generateJsonSchema(classOf[ModuleSpec])
        // Fix required properties for "Field"
        moduleSchema.get("definitions")
          .get("Field").asInstanceOf[ObjectNode]
          .replace("required", JsonNodeFactory.instance.arrayNode().add("name"))
        saveSchema(baseDir, "module.json", moduleSchema)

        val namespaceSchema = jsonSchemaGenerator.generateJsonSchema(classOf[NamespaceSpec])
        saveSchema(baseDir, "namespace.json", namespaceSchema)

        val projectSchema = jsonSchemaGenerator.generateJsonSchema(classOf[ProjectSpec])
        saveSchema(baseDir, "project.json", projectSchema)

      val documenterSchema = jsonSchemaGenerator.generateJsonSchema(classOf[DocumenterSpec])
      saveSchema(baseDir, "documentation.json", documenterSchema)
    }

    private def saveSchema(basedir:Path, name:String, jsonSchema:JsonNode) : Unit = {
        val mapper = new JacksonMapper(new JsonFactory())
        val writer = mapper.writer().withDefaultPrettyPrinter
        val schema = writer.writeValueAsString(jsonSchema)
        val file = new File(basedir.toFile, name)
        println(s"Generating Flowman YAML schema '${file.toString}'...'")
        val output = new FileOutputStream(file)
        val bytes = Charset.forName("UTF-8").encode(schema)
        output.write(bytes.array(), bytes.arrayOffset(), bytes.limit())
        output.close()
    }
}
