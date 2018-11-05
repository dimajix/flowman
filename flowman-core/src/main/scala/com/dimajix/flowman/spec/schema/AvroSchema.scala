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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.types.AvroSchemaUtils
import com.dimajix.flowman.types.Field


/**
  * Schema implementation for reading Avro schemas.
  */
class AvroSchema extends ExternalSchema {
    /**
      * Returns the description of the schema
      * @param context
      * @return
      */
    protected override def loadDescription(implicit context: Context): String = {
        avroSchema.getDoc
    }

    /**
      * Returns the list of all fields of the schema
      * @param context
      * @return
      */
    protected override def loadFields(implicit context: Context): Seq[Field] = {
        AvroSchemaUtils.fromAvro(avroSchema)
    }

    /**
      * Load and cache Avro schema from external source
      * @param context
      * @return
      */
    private def avroSchema(implicit context: Context) : org.apache.avro.Schema = {
        if (cachedAvroSchema == null) {
            val spec = loadSchemaSpec
            cachedAvroSchema = new org.apache.avro.Schema.Parser().parse(spec)
        }
        cachedAvroSchema
    }
    private var cachedAvroSchema : org.apache.avro.Schema = _
}
