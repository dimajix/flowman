/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.types

import java.nio.charset.Charset
import java.util.Locale

import com.dimajix.flowman.fs.File


/**
  * Helper class for exporting flowman schema definitions to files
  * @param fields
  */
class SchemaWriter(fields:Seq[Field]) {
    /**
      * Specifies the input data format format.
      */
    def format(format: String): SchemaWriter = {
        this.format = format
        this
    }

    def save(path: File): Unit = {
        writer(path)
    }

    private def writer : File => Unit = format.toLowerCase(Locale.ROOT) match {
        case "spark" => saveAsSpark
        case "avro" => saveAsAvro
        case _ => throw new IllegalArgumentException(s"Schema format $format not supported for export")
    }

    private def saveAsAvro(file:File) : Unit = {
        val schema = AvroSchemaUtils.toAvro(fields)
        writeSchemaFile(file, schema.toString(true))
    }

    private def saveAsSpark(file:File) : Unit = {
        val schema = SparkSchemaUtils.toSpark(fields)
        writeSchemaFile(file, schema.json)
    }

    private def writeSchemaFile(file:File, schema:String) : Unit = {
        // Manually convert string to UTF-8 and use write, since writeUTF apparently would write a BOM
        val bytes = Charset.forName("UTF-8").encode(schema)
        val output = file.create(true)
        output.write(bytes.array())
        output.close()
    }

    private var format: String = ""
}
