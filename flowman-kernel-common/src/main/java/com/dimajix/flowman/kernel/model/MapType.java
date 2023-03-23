/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman.kernel.model;

import lombok.EqualsAndHashCode;
import lombok.Value;


@Value
@EqualsAndHashCode(callSuper=false)
public class MapType extends FieldType {
    String typeName;
    FieldType keyType;
    FieldType valueType;


    public static MapType ofProto(com.dimajix.flowman.kernel.proto.MapType map) {
        return new MapType(
            map.getTypeName(),
            new ScalarType(map.getKeyType()),
            resolveValueType(map)
        );
    }

    private static FieldType resolveValueType(com.dimajix.flowman.kernel.proto.MapType sf) {
        if (sf.hasArray()) {
            return ArrayType.ofProto(sf.getArray());
        }
        else if (sf.hasMap()) {
            return MapType.ofProto(sf.getMap());
        }
        else if (sf.hasStruct()) {
            return StructType.ofProto(sf.getStruct());
        }
        else if (sf.hasScalar()) {
            return new ScalarType(sf.getScalar());
        }
        else {
            throw new IllegalArgumentException("Type of array not supported");
        }
    }
}
