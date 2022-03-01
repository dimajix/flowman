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
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.types.Field


object RelationSchema {
    def apply(context:Context, relation:String) : RelationSchema = {
        RelationSchema(Schema.Properties(context), RelationIdentifier(relation))
    }
}


case class RelationSchema(
    instanceProperties:Schema.Properties,
    relation: RelationIdentifier
) extends BaseSchema {
    private lazy val cachedFields = {
        val rel = context.getRelation(relation)
        rel.schema match {
            case Some(schema) => schema.fields ++ rel.partitions.map(_.field)
            case None =>
                val execution = context.execution
                execution.describe(rel).fields
        }
    }
    private lazy val cachedDescription = {
        val rel = context.getRelation(relation)
        rel.schema.flatMap(_.description).orElse(Some(s"Inferred from relation $relation"))
    }
    private lazy val cachedPrimaryKey = {
        val rel = context.getRelation(relation)
        rel.schema.toSeq.flatMap(_.primaryKey)
    }

    /**
     * Returns a list of physical resources required by this schema
     *
     * @return
     */
    override def requires: Set[ResourceIdentifier] = {
        val rel = context.getRelation(relation)
        rel.requires
    }

    /**
      * Returns the description of the schema
      *
      * @return
      */
    override def description: Option[String] = {
        cachedDescription
    }

    /**
      * Returns the list of all fields of the schema
      *
      * @return
      */
    override def fields: Seq[Field] = {
       cachedFields
    }

    /**
      * Returns the list of primary keys. Can be empty of no PK is available
      * @return
      */
    override def primaryKey : Seq[String] = {
        cachedPrimaryKey
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
            instanceProperties(context, relation),
            RelationIdentifier(context.evaluate(relation))
        )
    }
}
