/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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
import com.dimajix.flowman.execution.Environment
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema


case class SwaggerSchema(
    file: Environment => Option[Path] = _ => None,
    url: Environment => Option[URL] = _ => None,
    spec: Environment => Option[String] = _ => None,
    entity: Environment => Option[String] = _ => None,
    nullable: Environment => Boolean = _ => false
)
extends SchemaGen {
    override def apply(props:Schema.Properties) : schema.SwaggerSchema = {
        val env = props.context.environment
        schema.SwaggerSchema(
            props,
            file(env),
            url(env),
            spec(env),
            entity(env),
            nullable(env)
        )
    }
}
