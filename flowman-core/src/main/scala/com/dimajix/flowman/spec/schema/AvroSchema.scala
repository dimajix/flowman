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

import java.net.URL

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.types.AvroSchemaUtils
import com.dimajix.flowman.types.Field


/**
  * Schema implementation for reading Avro schemas.
  */
class AvroSchema extends Schema {
    @JsonProperty(value="file", required=false) private var _file: String = _
    @JsonProperty(value="url", required=false) private var _url: String = _
    @JsonProperty(value="spec", required=false) private var _spec: String = _

    def file(implicit context: Context) : File = Option(_file).map(context.evaluate).filter(_.nonEmpty).map(context.fs.file).orNull
    def url(implicit context: Context) : URL = Option(_url).map(context.evaluate).filter(_.nonEmpty).map(u => new URL(u)).orNull
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

        val avroSchema = if (file != null) {
            val input = file.open()
            try {
                new org.apache.avro.Schema.Parser().parse(input)
            }
            finally {
                input.close()
            }
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
