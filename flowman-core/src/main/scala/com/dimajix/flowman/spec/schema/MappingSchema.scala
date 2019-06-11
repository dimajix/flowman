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
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.StructType


object MappingSchema {
    def apply(context:Context, mapping:String) : MappingSchema = {
        MappingSchema(Schema.Properties(context), MappingOutputIdentifier(mapping))
    }
}


case class MappingSchema (
    instanceProperties:Schema.Properties,
    mapping: MappingOutputIdentifier
) extends Schema {
    /**
      * Returns the description of the schema
      * @return
      */
    override def description : Option[String] = Some(s"Inferred from mapping $mapping")

    /**
      * Returns the list of all fields of the schema
      * @return
      */
    override def fields : Seq[Field] = {
        val schemaCache = mutable.Map[MappingOutputIdentifier, StructType]()

        def describe(mapping:MappingOutputIdentifier) : StructType = {
            schemaCache.getOrElseUpdate(mapping, {
                val map = context.getMapping(MappingIdentifier(mapping.name, mapping.project))
                if (!map.outputs.contains(mapping.output))
                    throw new NoSuchElementException(s"Mapping ${map.identifier} does mot produce output '${mapping.output}'")
                val deps = map.dependencies
                    .map(id => (id,describe(id)))
                    .toMap
                map.describe(deps, mapping.output)
            })
        }

        describe(mapping).fields
    }

    /**
      * Returns the list of primary keys. Can be empty of no PK is available
      * @return
      */
    override def primaryKey : Seq[String] = Seq()
}



class MappingSchemaSpec extends SchemaSpec {
    @JsonProperty(value = "mapping", required = true) private var mapping: String = ""

    /**
      * Creates the instance of the specified Schema with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): MappingSchema = {
        MappingSchema(
            Schema.Properties(context),
            MappingOutputIdentifier(context.evaluate(mapping))
        )
    }
}
