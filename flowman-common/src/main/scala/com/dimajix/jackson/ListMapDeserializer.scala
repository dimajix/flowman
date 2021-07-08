/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.jackson

import scala.collection.immutable.ListMap
import scala.collection.mutable

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.deser.std.StdDeserializer


class ListMapDeserializer(vc:Class[_]) extends StdDeserializer[ListMap[String,String]](vc) {
    import java.io.IOException

    def this() = this(null)

    @throws[IOException]
    @throws[JsonProcessingException]
    def deserialize(jp: JsonParser, ctxt: DeserializationContext): ListMap[String,String] = {
        val result = mutable.ArrayBuffer[(String,String)]()
        var key:String = null
        if (jp.isExpectedStartObjectToken) {
            key = jp.nextFieldName
        }
        else {
            val t = jp.getCurrentToken
            if (t == JsonToken.END_OBJECT)
                return ListMap()
            if (t != JsonToken.FIELD_NAME)
                throw JsonMappingException.from(jp, "Wrong type for FieldType")
            key = jp.getCurrentName
        }


        while (key != null) {
            val t = jp.nextToken
            var value:String = null
            if (t == JsonToken.VALUE_NULL)
                value = null
            else if (t != JsonToken.START_OBJECT && t != JsonToken.START_ARRAY) {
                value = jp.getValueAsString
            } else {
                throw JsonMappingException.from(jp, "Wrong type for FieldType")
            }
            result.append((key, value))
            key = jp.nextFieldName
        }

        ListMap(result:_*)
    }
}
