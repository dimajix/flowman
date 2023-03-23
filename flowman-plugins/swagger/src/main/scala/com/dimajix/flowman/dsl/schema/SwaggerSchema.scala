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

package com.dimajix.flowman.dsl.schema

import java.net.URL

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.dsl.SchemaGen
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema


case class SwaggerSchema(
    file: Option[Path] = None,
    url: Option[URL] = None,
    spec: Option[String] = None,
    entity: Option[String] = None,
    nullable: Boolean = false
)
    extends SchemaGen {
    override def instantiate(context: Context, properties:Option[Schema.Properties] = None): schema.SwaggerSchema = {
        schema.SwaggerSchema(
            Schema.Properties(context),
            file.map(context.fs.file),
            url,
            spec,
            entity,
            nullable
        )
    }
}
