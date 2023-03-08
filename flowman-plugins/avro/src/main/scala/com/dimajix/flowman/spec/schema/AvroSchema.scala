/*
 * Copyright (C) 2018 The Flowman Authors
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
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.annotation.SchemaType
import com.dimajix.flowman.spec.schema.ExternalSchema.CachedSchema
import com.dimajix.flowman.util.AvroSchemaUtils


/**
  * Schema implementation for reading Avro schemas.
  */
case class AvroSchema(
    instanceProperties:Schema.Properties,
    override val file: Option[File],
    override val url: Option[URL],
    override val spec: Option[String],
    nullable: Boolean
) extends ExternalSchema {
    protected override val logger = LoggerFactory.getLogger(classOf[ExternalSchema])

    /**
      * Returns the description of the schema
      * @return
      */
    protected override def loadSchema : CachedSchema = {
        val spec = loadSchemaSpec
        val avroSchema = new org.apache.avro.Schema.Parser().parse(spec)
        CachedSchema(
            AvroSchemaUtils.fromAvro(avroSchema, nullable),
            Option(avroSchema.getDoc)
        )
    }
}



@SchemaType(kind="avro")
class AvroSchemaSpec extends ExternalSchemaSpec {
    @JsonSchemaInject(json="""{"type": [ "boolean", "string" ]}""")
    @JsonProperty(value="nullable", required=false) private var nullable: String = "false"

    /**
      * Creates the instance of the specified Schema with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Schema.Properties] = None): AvroSchema = {
        AvroSchema(
            instanceProperties(context, ""),
            file.map(context.evaluate).filter(_.nonEmpty).map(p => context.fs.file(p)),
            url.map(context.evaluate).filter(_.nonEmpty).map(u => new URL(u)),
            context.evaluate(spec),
            context.evaluate(nullable).toBoolean
        )
    }
}
