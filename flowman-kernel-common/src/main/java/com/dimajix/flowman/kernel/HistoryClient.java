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

package com.dimajix.flowman.kernel;

import com.dimajix.flowman.kernel.model.*;
import com.dimajix.flowman.kernel.proto.history.FindJobMetricsRequest;
import com.dimajix.flowman.kernel.proto.history.FindJobsRequest;
import com.dimajix.flowman.kernel.proto.history.FindTargetsRequest;
import com.dimajix.flowman.kernel.proto.history.GetJobMetricsRequest;
import com.dimajix.flowman.kernel.proto.history.HistoryServiceGrpc;
import io.grpc.ManagedChannel;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


public final class HistoryClient extends AbstractClient {
    private final Logger logger = LoggerFactory.getLogger(KernelClient.class);
    private final ManagedChannel channel;
    private final HistoryServiceGrpc.HistoryServiceBlockingStub blockingStub;

    public HistoryClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = HistoryServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public boolean isShutdown() {
        return channel.isTerminated();
    }

    @Override
    public boolean isTerminated() {
        return channel.isTerminated();
    }

    public Optional<JobState> getJob(String jobId) {
        val query = new JobQuery(
            Collections.singletonList(jobId),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            Optional.empty(),
            Optional.empty()
        );
        val jobs = findJobs(query, Collections.emptyList(), 1);
        return jobs.stream().findFirst();
    }
    public List<JobState> findJobs(JobQuery query, List<JobOrder> order, int maxRows) {
        val request = FindJobsRequest.newBuilder()
            .setMaxResults(maxRows)
            .setQuery(query.toProto())
            .addAllOrder(order.stream().map(JobOrder::toProto).collect(Collectors.toList()))
            .build();

        val result = call(() -> blockingStub.findJobs(request));
        return result.getJobsList().stream()
            .map(job ->
                new JobState(
                    job.getId(),
                    job.getNamespace(),
                    job.getProject(),
                    job.getVersion(),
                    job.getJob(),
                    Phase.ofProto(job.getPhase()),
                    job.getArgumentsMap(),
                    Status.ofProto(job.getStatus()),
                    job.hasStartDateTime() ? Optional.of(TypeConverters.toModel(job.getStartDateTime())) : Optional.empty(),
                    job.hasEndDateTime() ? Optional.of(TypeConverters.toModel(job.getEndDateTime())) : Optional.empty(),
                    job.hasError() ? Optional.of(job.getError()) : Optional.empty()
                )
            )
            .collect(Collectors.toList());
    }

    public Optional<TargetState> getTarget(String targetId) {
        val query = new TargetQuery(
            Collections.singletonList(targetId),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            Optional.empty(),
            Optional.empty()
        );
        val jobs = findTargets(query, Collections.emptyList(), 1);
        return jobs.stream().findFirst();
    }
    public List<TargetState> findTargets(TargetQuery query, List<TargetOrder> order, int maxRows) {
        val request = FindTargetsRequest.newBuilder()
            .setMaxResults(maxRows)
            .setQuery(query.toProto())
            .addAllOrder(order.stream().map(TargetOrder::toProto).collect(Collectors.toList()))
            .build();

        val result = call(() -> blockingStub.findTargets(request));
        return result.getTargetsList().stream()
            .map(target ->
                new TargetState(
                    target.getId(),
                    target.hasJobId() ? Optional.of(target.getJobId()) : Optional.empty(),
                    target.getNamespace(),
                    target.getProject(),
                    target.getVersion(),
                    target.getTarget(),
                    target.getPartitionsMap(),
                    Phase.ofProto(target.getPhase()),
                    Status.ofProto(target.getStatus()),
                    target.hasStartDateTime() ? Optional.of(TypeConverters.toModel(target.getStartDateTime())) : Optional.empty(),
                    target.hasEndDateTime() ? Optional.of(TypeConverters.toModel(target.getEndDateTime())) : Optional.empty(),
                    target.hasError() ? Optional.of(target.getError()) : Optional.empty()
                )
            )
            .collect(Collectors.toList());
    }

    public List<Measurement> getJobMetrics(String jobId) {
        val request = GetJobMetricsRequest.newBuilder()
            .setJobId(jobId)
            .build();

        val result = call(() -> blockingStub.getJobMetrics(request));
        return result.getMeasurementsList().stream()
            .map(Measurement::ofProto)
            .collect(Collectors.toList());
    }

    public List<MetricSeries> findJobMetrics(JobQuery query, List<String> groupings) {
        val request = FindJobMetricsRequest.newBuilder()
            .setQuery(query.toProto())
            .addAllGroupings(groupings)
            .build();

        val result = call(() -> blockingStub.findJobMetrics(request));
        return result.getMetricsList().stream()
            .map(MetricSeries::ofProto)
            .collect(Collectors.toList());
    }
}
