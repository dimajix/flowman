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
public class Target {
    Optional<String> project;
    String name;
    String kind;
    List<Phase> phases;
    List<TargetIdentifier> before;
    List<TargetIdentifier> after;

    Map<Phase,List<ResourceIdentifier>> requires;
    Map<Phase,List<ResourceIdentifier>> provides;


    public static Target ofProto(com.dimajix.flowman.kernel.proto.target.TargetDetails target) {
        val requires = new HashMap<Phase,List<ResourceIdentifier>>();
        putIfPresent(requires, Phase.VALIDATE, target.getRequiredByValidateList());
        putIfPresent(requires, Phase.CREATE, target.getRequiredByCreateList());
        putIfPresent(requires, Phase.BUILD, target.getRequiredByBuildList());
        putIfPresent(requires, Phase.VERIFY, target.getRequiredByVerifyList());
        putIfPresent(requires, Phase.TRUNCATE, target.getRequiredByTruncateList());
        putIfPresent(requires, Phase.DESTROY, target.getRequiredByDestroyList());

        val provides = new HashMap<Phase,List<ResourceIdentifier>>();
        putIfPresent(provides, Phase.VALIDATE, target.getProvidedByValidateList());
        putIfPresent(provides, Phase.CREATE, target.getProvidedByCreateList());
        putIfPresent(provides, Phase.BUILD, target.getProvidedByBuildList());
        putIfPresent(provides, Phase.VERIFY, target.getProvidedByVerifyList());
        putIfPresent(provides, Phase.TRUNCATE, target.getProvidedByTruncateList());
        putIfPresent(provides, Phase.DESTROY, target.getProvidedByDestroyList());

        return new Target(
            target.hasProject() ? Optional.of(target.getProject()) : Optional.empty(),
            target.getName(),
            target.getKind(),
            target.getPhasesList().stream().map(Phase::ofProto).collect(Collectors.toList()),
            target.getBeforeList().stream().map(TargetIdentifier::ofProto).collect(Collectors.toList()),
            target.getAfterList().stream().map(TargetIdentifier::ofProto).collect(Collectors.toList()),
            requires,
            provides
        );
    }

    private static void putIfPresent(Map<Phase,List<ResourceIdentifier>> out, Phase phase, List<com.dimajix.flowman.kernel.proto.ResourceIdentifier> ids) {
        if (!ids.isEmpty())
            out.put(phase,ids.stream().map(ResourceIdentifier::ofProto).collect(Collectors.toList()));
    }
}
