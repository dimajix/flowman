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

import java.util.Optional;

import lombok.Value;


@Value
public class StructField {
    String name;
    boolean nullable;
    Optional<String> description;
    Optional<String> format;
    Optional<String> collation;
    Optional<String> charset;
    String sqlType;
    FieldType type;

    public static StructField ofProto(com.dimajix.flowman.kernel.proto.StructField sf) {
        return new StructField(
            sf.getName(),
            sf.getNullable(),
            sf.hasDescription() ? Optional.of(sf.getDescription()) : Optional.empty(),
            sf.hasFormat() ? Optional.of(sf.getFormat()) : Optional.empty(),
            sf.hasCollation() ? Optional.of(sf.getCollation()) : Optional.empty(),
            sf.hasCharset() ? Optional.of(sf.getCharset()) : Optional.empty(),
            sf.getSqlType(),
            resolveProtoType(sf)
        );
    }

    private static FieldType resolveProtoType(com.dimajix.flowman.kernel.proto.StructField sf) {
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
            return ScalarType.ofProto(sf.getScalar());
        }
        else {
            throw new IllegalArgumentException("Type of field '" + sf.getName() + "' not supported (SQL type '" + sf.getSqlType() + "')");
        }
    }
}
