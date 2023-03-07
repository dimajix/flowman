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

import java.util.List;
import java.util.stream.Collectors;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.val;


@Value
@EqualsAndHashCode(callSuper=false)
public class StructType extends FieldType {
    String typeName;
    List<StructField> fields;

    public static StructType ofProto(com.dimajix.flowman.kernel.proto.StructType struct) {
        return new StructType(
            struct.getTypeName(),
            struct.getFieldsList().stream().map(StructField::ofProto).collect(Collectors.toList())
        );
    }

    public void printTree() {
        System.out.println(treeString());
    }

    public String treeString() {
        val builder = new StringBuilder();
        builder.append("root\n");
        val prefix = " |";
        fields.stream().forEach(field -> buildTreeString(field, prefix, builder));

        return builder.toString();
    }

    private void buildTreeString(StructField field, String prefix, StringBuilder builder) {
        val charset = field.getCharset().map(c -> ", charset = c" + c).orElse("");
        val collation = field.getCollation().map(c -> ", collation = " + c).orElse("");
        builder.append(prefix + "-- " + field.getName() + ":" + field.getType().getTypeName() + " (nullable=" + field.isNullable() + charset + collation + ")\n");
        buildTreeString(field.getType(), prefix + "    |", builder);
    }

    private void buildTreeString(FieldType type, String prefix, StringBuilder builder) {
        if (type instanceof StructType) {
            val struct = (StructType)type;
            struct.fields.stream().forEach(field -> buildTreeString(field, prefix, builder));
        }
        else if (type instanceof MapType) {
            val map = (MapType)type;
            builder.append(prefix + "-- key: " + map.getKeyType().getTypeName() + "\n");
            builder.append(prefix + "-- value: " + map.getValueType().getTypeName() +"\n");
            buildTreeString(map.getKeyType(), prefix + "    |", builder);
            buildTreeString(map.getValueType(), prefix + "    |", builder);
        }
        else if (type instanceof ArrayType) {
            val array = (ArrayType)type;
            builder.append(prefix + "-- element: " + array.getElementType().getTypeName() + "\n");
            buildTreeString(array.getElementType(), prefix + "    |", builder);
        }
    }
}
