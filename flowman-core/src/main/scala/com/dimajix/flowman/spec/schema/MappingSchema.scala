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

import scala.collection.mutable

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.StructType


object MappingSchema {
    def apply(mapping:String) : MappingSchema = {
        val result = new MappingSchema
        result._mapping = mapping
        result
    }
}


class MappingSchema extends Schema {
    @JsonProperty(value = "mapping", required = true) private var _mapping: String = ""

    def mapping(implicit context: Context): MappingIdentifier = MappingIdentifier(context.evaluate(_mapping))

    /**
      * Returns the description of the schema
      * @param context
      * @return
      */
    override def description(implicit context: Context) : String = s"Infered from mapping $mapping"

    /**
      * Returns the list of all fields of the schema
      * @param context
      * @return
      */
    override def fields(implicit context: Context) : Seq[Field] = {
        val schemaCache = mutable.Map[MappingIdentifier, StructType]()

        def describe(mapping:MappingIdentifier) : StructType = {
            schemaCache.getOrElseUpdate(mapping, {
                val map = context.getMapping(mapping)
                val deps = map.dependencies.map(id => (id,describe(id))).toMap
                map.describe(context, deps)
            })
        }

        describe(mapping).fields
    }

    /**
      * Returns the list of primary keys. Can be empty of no PK is available
      * @param context
      * @return
      */
    override def primaryKey(implicit context: Context) : Seq[String] = Seq()
}
