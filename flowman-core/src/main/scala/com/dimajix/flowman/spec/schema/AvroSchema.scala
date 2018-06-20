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

import java.io.File
import java.net.URL

import scala.collection.JavaConversions._

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.avro.Schema.Type.RECORD

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.util.AvroSchemaUtils

/**
  * Schema implementation for reading Avro schemas.
  */
class AvroSchema extends Schema {
    @JsonProperty(value="file", required=false) private var _file: String = _
    @JsonProperty(value="url", required=false) private var _url: String = _
    @JsonProperty(value="spec", required=false) private var _spec: String = _

    def file(implicit context: Context) : String = context.evaluate(_file)
    def url(implicit context: Context) : URL = if (_url != null && _url.nonEmpty) new URL(context.evaluate(_url)) else null
    def spec(implicit context: Context) : String = context.evaluate(_spec)

    /**
      * Returns the description of the schema
      * @param context
      * @return
      */
    override def description(implicit context: Context): String = {
        loadAvroSchema.getDoc
    }

    /**
      * Returns the list of all fields of the schema
      * @param context
      * @return
      */
    override def fields(implicit context: Context): Seq[Field] = {
        AvroSchemaUtils.fromAvro(loadAvroSchema)
    }

    private def loadAvroSchema(implicit context: Context) : org.apache.avro.Schema = {
        val file = this.file
        val url = this.url
        val spec = this.spec

        val avroSchema = if (file != null && file.nonEmpty) {
            new org.apache.avro.Schema.Parser().parse(new File(file))
        }
        else if (url != null) {
            val con = url.openConnection()
            con.setUseCaches(false)
            new org.apache.avro.Schema.Parser().parse(con.getInputStream)
        }
        else if (spec != null && spec.nonEmpty) {
            new org.apache.avro.Schema.Parser().parse(spec)
        }
        else {
            throw new IllegalArgumentException("An Avro schema needs either a 'file', 'url' or a 'spec' element")
        }

        avroSchema
    }
}
