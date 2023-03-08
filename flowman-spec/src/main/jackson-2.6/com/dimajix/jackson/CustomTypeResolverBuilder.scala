/*
 * Copyright (C) 2021 The Flowman Authors
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

import java.util

import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.cfg.MapperConfig
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder
import com.fasterxml.jackson.databind.jsontype.impl.TypeNameIdResolver


class CustomTypeResolverBuilder extends StdTypeResolverBuilder {
    override protected def idResolver(config: MapperConfig[_], baseType: JavaType, subtypes: util.Collection[NamedType], forSer: Boolean, forDeser: Boolean) : TypeIdResolver = {
        val resolver = TypeNameIdResolver.construct(config, baseType, subtypes, forSer, forDeser)
        wrapIdResolver(resolver, baseType)
    }

    protected def wrapIdResolver(resolver:TypeIdResolver, baseType: JavaType) : TypeIdResolver = resolver
}
