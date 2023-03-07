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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.Value;
import lombok.val;


@Value
public class Relation {
    Optional<String> project;
    String name;
    String kind;
    Map<Operation, List<ResourceIdentifier>> requires;
    Map<Operation, List<ResourceIdentifier>> provides;

    public static Relation ofProto(com.dimajix.flowman.kernel.proto.relation.RelationDetails relation) {
        val requires = new HashMap<Operation, List<ResourceIdentifier>>();
        putIfPresent(requires, Operation.CREATE, relation.getRequiredByCreateList());
        putIfPresent(requires, Operation.READ, relation.getRequiredByReadList());
        putIfPresent(requires, Operation.WRITE, relation.getRequiredByWriteList());

        val provides = new HashMap<Operation, List<ResourceIdentifier>>();
        putIfPresent(provides, Operation.CREATE, relation.getProvidedByCreateList());
        putIfPresent(provides, Operation.READ, relation.getProvidedByReadList());
        putIfPresent(provides, Operation.WRITE, relation.getProvidedByWriteList());

        return new Relation(
            relation.hasProject() ? Optional.of(relation.getProject()) : Optional.empty(),
            relation.getName(),
            relation.getKind(),
            requires,
            provides
        );
    }

    private static void putIfPresent(Map<Operation,List<ResourceIdentifier>> out, Operation op, List<com.dimajix.flowman.kernel.proto.ResourceIdentifier> ids) {
        if (!ids.isEmpty())
            out.put(op,ids.stream().map(ResourceIdentifier::ofProto).collect(Collectors.toList()));
    }
}
