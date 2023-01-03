/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.annotation.SchemaType
import com.dimajix.flowman.spec.schema.ExternalSchema.CachedSchema


/**
  * Schema implementation for reading Swagger / OpenAPI schemas. This implementation will preserve the ordering of
  * fields.
  */
case class OpenApiSchema(
    instanceProperties:Schema.Properties,
    override val file: Option[File],
    override val url: Option[URL],
    override val spec: Option[String],
    entity: Option[String],
    nullable: Boolean
) extends ExternalSchema {
    protected override val logger = LoggerFactory.getLogger(classOf[OpenApiSchema])

    /**
      * Returns the list of all fields of the schema
      * @return
      */
    protected override def loadSchema : CachedSchema = {
        val string = loadSchemaSpec
        val api = OpenApiSchemaUtils.parse(string)

        CachedSchema(
            OpenApiSchemaUtils.fromOpenApi(api, entity, nullable),
            Option(api.getInfo).map(_.getDescription)
        )
    }
}


@SchemaType(kind="openApi")
class OpenApiSchemaSpec extends ExternalSchemaSpec {
    @JsonProperty(value="entity", required=false) private var entity: Option[String] = None
    @JsonProperty(value="nullable", required=false) private var nullable: String = "false"

    /**
      * Creates the instance of the specified Schema with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Schema.Properties]): OpenApiSchema = {
        OpenApiSchema(
            instanceProperties(context, file.getOrElse("")),
            file.map(context.evaluate).filter(_.nonEmpty).map(p => context.fs.file(p)),
            url.map(context.evaluate).filter(_.nonEmpty).map(u => new URL(u)),
            context.evaluate(spec),
            entity.map(context.evaluate),
            context.evaluate(nullable).toBoolean
        )
    }
}
