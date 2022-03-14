/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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
import com.dimajix.flowman.model.BaseSchema
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.SchemaUtils
import com.dimajix.flowman.types.StructType


case class UnionSchema(
    instanceProperties:Schema.Properties,
    schemas:Seq[Schema]
) extends BaseSchema {
    private val unionSchema = SchemaUtils.union(schemas.map(s => StructType(s.fields)))
    private val unionDescription = schemas.flatMap(_.description).find(_.nonEmpty)

    /**
     * Returns a list of physical resources required by this schema
     *
     * @return
     */
    override def requires : Set[ResourceIdentifier] = schemas.flatMap(_.requires).toSet

    /**
      * Returns the description of the schema
      * @return
      */
    override def description : Option[String] = unionDescription

    /**
      * Returns the list of all fields of the schema
      * @return
      */
    override def fields : Seq[Field] = {
        unionSchema.fields
    }

    /**
      * Returns the list of primary keys. Can be empty of no PK is available
      * @return
      */
    override def primaryKey : Seq[String] = Seq()
}


class UnionSchemaSpec extends SchemaSpec {
    @JsonProperty(value = "schemas", required = true) private var schema: Seq[SchemaSpec] = Seq()

    /**
      * Creates the instance of the specified Schema with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Schema.Properties] = None): UnionSchema = {
        UnionSchema(
            instanceProperties(context, ""),
            schema.map(_.instantiate(context))
        )
    }
}
