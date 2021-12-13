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

import java.io.IOException

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.DatabindContext
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver


class WrappingTypeIdResolver(wrapped:TypeIdResolver) extends TypeIdResolver {
    override def init(baseType: JavaType): Unit = wrapped.init(baseType)
    override def idFromValue(value: Any): String = wrapped.idFromValue(value)
    override def idFromValueAndType(value: Any, suggestedType: Class[_]): String = wrapped.idFromValueAndType(value, suggestedType)
    override def idFromBaseType(): String = wrapped.idFromBaseType()
    @throws[IOException]
    override def typeFromId(context: DatabindContext, id: String): JavaType = wrapped.typeFromId(context, id)
    override def getDescForKnownTypeIds: String = wrapped.getDescForKnownTypeIds
    override def getMechanism: JsonTypeInfo.Id = wrapped.getMechanism
}
