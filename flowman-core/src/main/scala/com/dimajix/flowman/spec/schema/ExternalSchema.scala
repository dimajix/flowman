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
import org.slf4j.Logger

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.spec.schema.ExternalSchema.CachedSchema
import com.dimajix.flowman.types.Field


object ExternalSchema {
    case class CachedSchema(
        fields:Seq[Field],
        description: String,
        primaryKey: Seq[String] = Seq()
    )
}

/**
  * Helper class for external schemas which are stored in files or at URLs
  */
abstract class ExternalSchema extends Schema {
    protected val logger: Logger
    protected val file: File
    protected val url: URL
    protected val spec: String

    /**
      * Returns the description of the schema. This will be cached once and for ever
      * @return
      */
    override def description : String = {
        cache.description
    }

    /**
      * Returns the list of all fields of the schema. This list will be cached once and for ever
      * @return
      */
    override def fields : Seq[Field] = {
        cache.fields
    }

    override def primaryKey : Seq[String] = {
        cache.primaryKey
    }

    private def cache : CachedSchema = {
        if (cachedSchema == null) {
            cachedSchema = loadSchema
        }
        cachedSchema
    }
    private var cachedSchema:CachedSchema = _

    /**
      * Loads the real schema from external source. This has to be provided by derived classes
      * @return
      */
    protected def loadSchema : CachedSchema

    /**
      * Loads the raw schema definition from the external resource (file or URL)
      *
      * @return
      */
    protected def loadSchemaSpec : String = {
        if (file != null) {
            logger.info(s"Loading schema from file $file")
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
            logger.info(s"Loading schema from url $url")
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



/**
  * Helper class for external schemas which are stored in files or at URLs
  */
abstract class ExternalSchemaSpec extends SchemaSpec {
    @JsonProperty(value = "file", required = false) protected var file: String = _
    @JsonProperty(value = "url", required = false) protected var url: String = _
    @JsonProperty(value = "spec", required = false) protected var spec: String = _
}
