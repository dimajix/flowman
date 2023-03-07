/*
 * Copyright 2018-2023 Kaya Kupferschmidt
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
public class ArrayType extends FieldType {
    String typeName;
    FieldType elementType;

    public static ArrayType ofProto(com.dimajix.flowman.kernel.proto.ArrayType array) {
        return new ArrayType(array.getTypeName(), resolveProtoType(array));
    }

    private static FieldType resolveProtoType(com.dimajix.flowman.kernel.proto.ArrayType sf) {
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
