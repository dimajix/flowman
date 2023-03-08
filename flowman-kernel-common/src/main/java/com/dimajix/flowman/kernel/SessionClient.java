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

package com.dimajix.flowman.kernel;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import lombok.val;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;

import com.dimajix.flowman.grpc.ExceptionUtils;
import com.dimajix.flowman.kernel.model.*;
import com.dimajix.flowman.kernel.proto.JobContext;
import com.dimajix.flowman.kernel.proto.LogEvent;
import com.dimajix.flowman.kernel.proto.LogLevel;
import com.dimajix.flowman.kernel.proto.TestContext;
import com.dimajix.flowman.kernel.proto.job.ExecuteJobRequest;
import com.dimajix.flowman.kernel.proto.job.GetJobRequest;
import com.dimajix.flowman.kernel.proto.job.ListJobsRequest;
import com.dimajix.flowman.kernel.proto.mapping.DescribeMappingRequest;
import com.dimajix.flowman.kernel.proto.mapping.GetMappingRequest;
import com.dimajix.flowman.kernel.proto.mapping.ListMappingsRequest;
import com.dimajix.flowman.kernel.proto.project.GetProjectRequest;
import com.dimajix.flowman.kernel.proto.relation.DescribeRelationRequest;
import com.dimajix.flowman.kernel.proto.relation.ExecuteRelationRequest;
import com.dimajix.flowman.kernel.proto.relation.GetRelationRequest;
import com.dimajix.flowman.kernel.proto.relation.ListRelationsRequest;
import com.dimajix.flowman.kernel.proto.session.DeleteSessionRequest;
import com.dimajix.flowman.kernel.proto.session.EnterContextRequest;
import com.dimajix.flowman.kernel.proto.session.GetContextRequest;
import com.dimajix.flowman.kernel.proto.session.GetSessionRequest;
import com.dimajix.flowman.kernel.proto.session.LeaveContextRequest;
import com.dimajix.flowman.kernel.proto.session.SessionServiceGrpc;
import com.dimajix.flowman.kernel.proto.session.SubscribeLogRequest;
import com.dimajix.flowman.kernel.proto.target.ExecuteTargetRequest;
import com.dimajix.flowman.kernel.proto.target.GetTargetRequest;
import com.dimajix.flowman.kernel.proto.target.ListTargetsRequest;
import com.dimajix.flowman.kernel.proto.test.GetTestRequest;
import com.dimajix.flowman.kernel.proto.test.ListTestsRequest;


public final class SessionClient extends AbstractClient {
    private final ManagedChannel channel;
    private final SessionServiceGrpc.SessionServiceBlockingStub blockingStub;
    private final SessionServiceGrpc.SessionServiceStub asyncStub;
    private final String sessionId;


    public SessionClient(ManagedChannel channel, String sessionId) {
        this.channel = channel;
        this.sessionId = sessionId;
        blockingStub = SessionServiceGrpc.newBlockingStub(channel);
        asyncStub = SessionServiceGrpc.newStub(channel);
    }

    public String getSessionId() {
        return sessionId;
    }

    public Session getSession() {
        val request = GetSessionRequest.newBuilder()
            .setSessionId(sessionId)
            .build();
        val result = call(() -> blockingStub.getSession(request));
        val session = result.getSession();
        return Session.ofProto(session);
    }

    public Project getProject() {
        val request = GetProjectRequest.newBuilder()
            .setSessionId(sessionId)
            .build();
        val result = call(() -> blockingStub.getProject(request));
        val project = result.getProject();
        return Project.ofProto(project);
    }

    public void subscribeLog(Consumer<LoggingEvent> consumer) {
        val request = SubscribeLogRequest.newBuilder()
                .setSessionId(sessionId)
                .build();
        asyncStub.subscribeLog(request, new StreamObserver<LogEvent>() {
            @Override
            public void onNext(LogEvent value) {
                val event = new LoggingEvent() {
                    @Override
                    public Level getLevel() {
                        switch(value.getLevel()) {
                            case TRACE:
                                return Level.TRACE;
                            case DEBUG:
                                return Level.DEBUG;
                            case INFO:
                                return Level.INFO;
                            case WARN:
                                return Level.WARN;
                            case ERROR:
                                return Level.ERROR;
                            default:
                                return Level.ERROR;
                        }
                    }
                    @Override
                    public Marker getMarker() {
                        return null;
                    }
                    @Override
                    public String getLoggerName() {
                        return value.getLogger();
                    }
                    @Override
                    public String getMessage() {
                        return value.getMessage();
                    }
                    @Override
                    public String getThreadName() {
                        return null;
                    }
                    @Override
                    public Object[] getArgumentArray() {
                        return new Object[0];
                    }
                    @Override
                    public long getTimeStamp() {
                        val ts = value.getTimestamp();
                        return ts.getSeconds() * 1000 + ts.getNanos() / 1000000;
                    }
                    @Override
                    public Throwable getThrowable() {
                        if (value.hasException()) {
                            return ExceptionUtils.unwrap(value.getException());
                        }
                        else return null;
                    }
                };
                consumer.accept(event);
            }
            @Override
            public void onError(Throwable t) {
            }
            @Override
            public void onCompleted() {
            }
        });
    }

    public void enterJobContext(String job, Map<String,String> arguments) {
        val request = EnterContextRequest.newBuilder()
            .setSessionId(sessionId)
            .setJob(
                JobContext.newBuilder()
                    .setJob(JobIdentifier.ofString(job).toProto())
                    .putAllArguments(arguments)
                    .build()
            )
            .build();
        call(() -> blockingStub.enterContext(request));
    }
    public void enterTestContext(String test) {
        val request = EnterContextRequest.newBuilder()
            .setSessionId(sessionId)
            .setTest(
                TestContext.newBuilder()
                    .setTest(TestIdentifier.ofString(test).toProto())
                    .build()
            )
            .build();
        call(() -> blockingStub.enterContext(request));
    }
    public void leaveContext() {
        val request = LeaveContextRequest.newBuilder()
            .setSessionId(sessionId)
            .build();
        call(() -> blockingStub.leaveContext(request));
    }

    public Context getContext() {
        val request = GetContextRequest.newBuilder()
            .setSessionId(sessionId)
            .build();
        val result = call(() -> blockingStub.getContext(request));
        Optional<String> project = result.hasProject() ? Optional.of(result.getProject()) : Optional.empty();
        if (result.hasJob()) {
            val job = result.getJob();
            return new Context(project, Optional.of(JobIdentifier.ofProto(job.getJob())), Optional.empty());
        }
        else if (result.hasTest()) {
            val test = result.getTest();
            return new Context(project, Optional.empty(), Optional.of(TestIdentifier.ofProto(test.getTest())));
        }
        else {
            return new Context(project, Optional.empty(), Optional.empty());
        }
    }

    public void shutdown() {
        val request = DeleteSessionRequest.newBuilder()
            .setSessionId(sessionId)
            .build();
        call(() -> blockingStub.deleteSession(request));
    }

    public List<MappingIdentifier> listMappings() {
        val request = ListMappingsRequest.newBuilder()
            .setSessionId(sessionId)
            .build();
        val result = call(() -> blockingStub.listMappings(request));
        return result.getMappingsList().stream().map(MappingIdentifier::ofProto).collect(Collectors.toList());
    }
    public Mapping getMapping(MappingIdentifier mappingId) {
        val request = GetMappingRequest.newBuilder()
            .setSessionId(sessionId)
            .setMapping(mappingId.toProto())
            .build();
        val result = call(() -> blockingStub.getMapping(request));
        val mapping = result.getMapping();

        return Mapping.ofProto(mapping);
    }
    public StructType describeMapping(MappingIdentifier mappingId, String output, boolean useSpark) {
        val request = DescribeMappingRequest.newBuilder()
            .setSessionId(sessionId)
            .setMapping(mappingId.toProto())
            .setOutput(output)
            .setUseSpark(useSpark)
            .build();
        val result = call(() -> blockingStub.describeMapping(request));
        val schema = result.getSchema();

        return StructType.ofProto(schema);
    }

    public List<RelationIdentifier> listRelations() {
        val request = ListRelationsRequest.newBuilder()
            .setSessionId(sessionId)
            .build();
        val result = call(() -> blockingStub.listRelations(request));
        return result.getRelationsList().stream().map(RelationIdentifier::ofProto).collect(Collectors.toList());
    }

    public Relation getRelation(RelationIdentifier relationId) {
        val request = GetRelationRequest.newBuilder()
            .setSessionId(sessionId)
            .setRelation(relationId.toProto())
            .build();
        val result = call(() -> blockingStub.getRelation(request));
        val relation = result.getRelation();

        return Relation.ofProto(relation);
    }

    public StructType describeRelation(RelationIdentifier relationId, Map<String,String> partition, boolean useSpark) {
        val request = DescribeRelationRequest.newBuilder()
            .setSessionId(sessionId)
            .setRelation(relationId.toProto())
            .putAllPartition(partition)
            .setUseSpark(useSpark)
            .build();
        val result = call(() -> blockingStub.describeRelation(request));
        val schema = result.getSchema();

        return StructType.ofProto(schema);
    }
    public Status executeRelations(List<RelationIdentifier> targetIds, Phase phase, Map<String,String> partition, boolean force, boolean keepGoing, boolean dryRun) {
        val request = ExecuteRelationRequest.newBuilder()
            .setSessionId(sessionId)
            .addAllRelations(targetIds.stream().map(RelationIdentifier::toProto).collect(Collectors.toList()))
            .putAllPartition(partition)
            .setPhase(phase.toProto())
            .setForce(force)
            .setKeepGoing(keepGoing)
            .setDryRun(dryRun)
            .build();

        val result = call(() -> blockingStub.executeRelation(request));
        val status = result.getStatus();

        return Status.ofProto(status);
    }

    public List<TestIdentifier> listTests() {
        val request = ListTestsRequest.newBuilder()
            .setSessionId(sessionId)
            .build();
        val result = call(() -> blockingStub.listTests(request));
        return result.getTestsList().stream().map(TestIdentifier::ofProto).collect(Collectors.toList());
    }
    public Test getTest(TestIdentifier testId) {
        val request = GetTestRequest.newBuilder()
            .setSessionId(sessionId)
            .setTest(testId.toProto())
            .build();
        val result = call(() -> blockingStub.getTest(request));
        val test = result.getTest();

        return Test.ofProto(test);
    }

    public List<TargetIdentifier> listTargets() {
        val request = ListTargetsRequest.newBuilder()
            .setSessionId(sessionId)
            .build();
        val result = call(() -> blockingStub.listTargets(request));
        return result.getTargetsList().stream().map(TargetIdentifier::ofProto).collect(Collectors.toList());
    }
    public Target getTarget(TargetIdentifier targetId) {
        val request = GetTargetRequest.newBuilder()
            .setSessionId(sessionId)
            .setTarget(targetId.toProto())
            .build();
        val result = call(() -> blockingStub.getTarget(request));
        val target = result.getTarget();

        return Target.ofProto(target);
    }
    public Status executeTargets(List<TargetIdentifier> targetIds, List<Phase> lifecycle, boolean force, boolean keepGoing, boolean dryRun) {
        val request = ExecuteTargetRequest.newBuilder()
            .setSessionId(sessionId)
            .addAllTargets(targetIds.stream().map(TargetIdentifier::toProto).collect(Collectors.toList()))
            .addAllPhases(lifecycle.stream().map(Phase::toProto).collect(Collectors.toList()))
            .setForce(force)
            .setKeepGoing(keepGoing)
            .setDryRun(dryRun)
            .build();

        val result = call(() -> blockingStub.executeTarget(request));
        val status = result.getStatus();

        return Status.ofProto(status);
    }

    public List<JobIdentifier> listJobs() {
        val request = ListJobsRequest.newBuilder()
            .setSessionId(sessionId)
            .build();
        val result = call(() -> blockingStub.listJobs(request));
        return result.getJobsList().stream().map(JobIdentifier::ofProto).collect(Collectors.toList());
    }

    public Job getJob(JobIdentifier jobId) {
        val request = GetJobRequest.newBuilder()
            .setSessionId(sessionId)
            .setJob(jobId.toProto())
            .build();
        val result = call(() -> blockingStub.getJob(request));
        val job = result.getJob();

        return Job.ofProto(job);
    }

    public Status executeJob(JobIdentifier jobId, List<Phase> lifecycle, Map<String,String> args, List<String> targets,List<String> dirtyTargets,boolean force, boolean keepGoing, boolean dryRun,int parallelism) {
        val request = ExecuteJobRequest.newBuilder()
            .setSessionId(sessionId)
            .setJob(jobId.toProto())
            .addAllPhases(lifecycle.stream().map(Phase::toProto).collect(Collectors.toList()))
            .putAllArguments(args)
            .addAllTargets(targets)
            .addAllDirtyTargets(dirtyTargets)
            .setForce(force)
            .setKeepGoing(keepGoing)
            .setDryRun(dryRun)
            .setParallelism(parallelism)
            .build();

        val result = call(() -> blockingStub.executeJob(request));
        val status = result.getStatus();

        return Status.ofProto(status);
    }
}
