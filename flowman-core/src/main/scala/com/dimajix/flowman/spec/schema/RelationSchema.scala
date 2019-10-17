/*
 * Copyright 2019 Kaya Kupferschmidt
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
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.types.Field


case class RelationSchema(
    instanceProperties:Schema.Properties,
    relation: RelationIdentifier
) extends Schema {
    /**
      * Returns the description of the schema
      *
      * @return
      */
    override def description: Option[String] = {
        val rel = context.getRelation(relation)

        rel.schema.flatMap(_.description)
    }

    /**
      * Returns the list of all fields of the schema
      *
      * @return
      */
    override def fields: Seq[Field] = {
        val rel = context.getRelation(relation)

        rel.schema.toSeq.flatMap(_.fields)
    }

    /**
      * Returns the list of primary keys. Can be empty of no PK is available
      * @return
      */
    override def primaryKey : Seq[String] = {
        val rel = context.getRelation(relation)

        rel.schema.toSeq.flatMap(_.primaryKey)
    }
}



class RelationSchemaSpec extends SchemaSpec {
    @JsonProperty(value = "relation", required = true) private var relation: String = ""

    /**
      * Creates the instance of the specified Schema with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): RelationSchema = {
        RelationSchema(
            Schema.Properties(context),
            RelationIdentifier(context.evaluate(relation))
        )
    }
}
