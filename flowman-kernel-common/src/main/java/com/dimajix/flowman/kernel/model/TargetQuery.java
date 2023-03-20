/*
 * Copyright (C) 2023 The Flowman Authors
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

import com.dimajix.flowman.kernel.proto.history.TargetHistoryQuery;
import lombok.Value;
import lombok.val;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


@Value
public class TargetQuery {
    List<String> ids;
    List<String> namespaces;
    List<String> projects;
    List<String> jobs;
    List<String> jobIds;
    List<String> targets;
    List<Status> status;
    List<Phase> phases;
    Map<String,String> partitions;
    Optional<ZonedDateTime> from;
    Optional<ZonedDateTime> until;

    public TargetHistoryQuery toProto() {
        val pquery = TargetHistoryQuery.newBuilder();
        pquery.addAllId(getIds());
        pquery.addAllNamespace(getNamespaces());
        pquery.addAllProject(getProjects());
        pquery.addAllTarget(getTargets());
        pquery.addAllStatus(getStatus().stream().map(Status::toProto).collect(Collectors.toList()));
        pquery.addAllPhase(getPhases().stream().map(Phase::toProto).collect(Collectors.toList()));
        pquery.putAllPartitions(getPartitions());
        getFrom().ifPresent(dt -> pquery.setFrom(TypeConverters.toProto(dt)));
        getUntil().ifPresent(dt -> pquery.setUntil(TypeConverters.toProto(dt)));
        return pquery.build();
    }
}
