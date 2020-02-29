/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.AbstractInstance
import com.dimajix.flowman.model.Instance
import com.dimajix.flowman.spec.AbstractInstance
import com.dimajix.flowman.types.Field


case class EmbeddedSchema(
    instanceProperties : Schema.Properties,
    description : Option[String],
    fields : Seq[Field],
    primaryKey : Seq[String]
)
extends AbstractInstance with Schema {
}



class EmbeddedSchemaSpec extends SchemaSpec {
    @JsonProperty(value="fields", required=false) private var fields: Seq[Field] = _
    @JsonProperty(value="description", required = false) private var description: Option[String] = None
    @JsonProperty(value="primaryKey", required = false) private var primaryKey: Seq[String] = Seq()

    /**
      * Creates the instance of the specified Schema with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): EmbeddedSchema = {
        EmbeddedSchema(
            Schema.Properties(context),
            description.map(context.evaluate),
            fields,
            primaryKey.map(context.evaluate)
        )
    }
}
