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

package com.dimajix.flowman.spec.schema

import java.io.StringWriter
import java.net.URL
import java.nio.charset.Charset

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.commons.io.IOUtils

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.types.Field


/**
  * Helper class for external schemas which are stored in files or at URLs
  */
abstract class ExternalSchema extends Schema {
    @JsonProperty(value = "file", required = false) private var _file: String = _
    @JsonProperty(value = "url", required = false) private var _url: String = _
    @JsonProperty(value = "spec", required = false) private var _spec: String = _

    def file(implicit context: Context): File = Option(_file).map(context.evaluate).filter(_.nonEmpty).map(context.fs.file).orNull
    def url(implicit context: Context): URL = Option(_url).map(context.evaluate).filter(_.nonEmpty).map(u => new URL(u)).orNull
    def spec(implicit context: Context): String = context.evaluate(_spec)

    /**
      * Returns the description of the schema. This will be cached once and for ever
      * @param context
      * @return
      */
    override def description(implicit context: Context): String = {
        if (cachedDescription == null) {
            cachedDescription = loadDescription
        }
        cachedDescription
    }
    private var cachedDescription: String = _

    /**
      * Returns the list of all fields of the schema. This list will be cached once and for ever
      * @param context
      * @return
      */
    override def fields(implicit context: Context): Seq[Field] = {
        if (cachedFields == null) {
            cachedFields = loadFields
        }
        cachedFields
    }
    private var cachedFields: Seq[Field] = _

    /**
      * Load description from external source. This has to be provided by derived classes
      * @param context
      * @return
      */
    protected def loadDescription(implicit context: Context): String

    /**
      * Load list of fields from external source. This has to be provided by derived classes
      * @param context
      * @return
      */
    protected def loadFields(implicit context: Context): Seq[Field]

    /**
      * Loads the raw schema definition from the external resource (file or URL)
      *
      * @param context
      * @return
      */
    protected def loadSchemaSpec(implicit context: Context): String = {
        val file = this.file
        val url = this.url
        val spec = this.spec

        if (file != null) {
            val input = file.open()
            try {
                val writer = new StringWriter()
                IOUtils.copy(input, writer, Charset.forName("UTF-8"))
                writer.toString
            }
            finally {
                input.close()
            }
        }
        else if (url != null) {
            IOUtils.toString(url)
        }
        else if (spec != null && spec.nonEmpty) {
            spec
        }
        else {
            throw new IllegalArgumentException("A schema needs either a 'file', 'url' or a 'spec' element")
        }
    }
}
