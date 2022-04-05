/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.relation

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.RelationReference
import com.dimajix.flowman.spec.Spec


@JsonDeserialize(using=classOf[RelationReferenceDeserializer])
@JsonSchemaInject(
  merge = false,
  json = """
      {
        "type": [ "object", "string" ],
        "oneOf": [
          { "type": "string" },
          { "$ref": "#/definitions/RelationSpec" }
        ]
      }
      """
)
abstract class RelationReferenceSpec {
    def instantiate(context: Context): Reference[Relation]
}
final case class IdentifierRelationReferenceSpec(relation:String) extends RelationReferenceSpec {
    override def instantiate(context: Context): Reference[Relation] = {
        val id = RelationIdentifier.parse(context.evaluate(relation))
        RelationReference(context,id)
    }
}
final case class ValueRelationReferenceSpec(relation:Prototype[Relation]) extends RelationReferenceSpec {
    override def instantiate(context: Context): Reference[Relation] = {
        RelationReference(context, relation)
    }
}


private class RelationReferenceDeserializer(vc:Class[_]) extends StdDeserializer[RelationReferenceSpec](vc) {
    import java.io.IOException

    def this() = this(null)

    @throws[IOException]
    @throws[JsonProcessingException]
    def deserialize(jp: JsonParser, ctxt: DeserializationContext): RelationReferenceSpec = {
        jp.getCurrentToken match {
            case JsonToken.VALUE_STRING => {
                IdentifierRelationReferenceSpec(jp.getText)
            }
            case JsonToken.START_OBJECT => {
                val spec = jp.readValueAs(classOf[RelationSpec])
                ValueRelationReferenceSpec(spec)
            }
            case _ => throw JsonMappingException.from(jp, "Wrong type for RelationReference")
        }
    }
}
