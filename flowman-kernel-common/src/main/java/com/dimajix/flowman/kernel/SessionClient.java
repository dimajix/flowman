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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.dimajix.flowman.grpc.ExceptionUtils;
import com.dimajix.flowman.kernel.proto.documentation.GenerateDocumentationRequest;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;

import com.dimajix.flowman.kernel.model.*;
import com.dimajix.flowman.kernel.proto.JobContext;
import com.dimajix.flowman.kernel.proto.LogEvent;
import com.dimajix.flowman.kernel.proto.TestContext;
import com.dimajix.flowman.kernel.proto.job.ExecuteJobRequest;
import com.dimajix.flowman.kernel.proto.job.GetJobRequest;
import com.dimajix.flowman.kernel.proto.job.ListJobsRequest;
import com.dimajix.flowman.kernel.proto.mapping.*;
import com.dimajix.flowman.kernel.proto.project.ExecuteProjectRequest;
import com.dimajix.flowman.kernel.proto.project.GetProjectRequest;
import com.dimajix.flowman.kernel.proto.relation.DescribeRelationRequest;
import com.dimajix.flowman.kernel.proto.relation.ExecuteRelationRequest;
import com.dimajix.flowman.kernel.proto.relation.GetRelationRequest;
import com.dimajix.flowman.kernel.proto.relation.ListRelationsRequest;
import com.dimajix.flowman.kernel.proto.relation.ReadRelationRequest;
import com.dimajix.flowman.kernel.proto.session.*;
import com.dimajix.flowman.kernel.proto.target.ExecuteTargetRequest;
import com.dimajix.flowman.kernel.proto.target.GetTargetRequest;
import com.dimajix.flowman.kernel.proto.target.ListTargetsRequest;
import com.dimajix.flowman.kernel.proto.test.ExecuteTestRequest;
import com.dimajix.flowman.kernel.proto.test.GetTestRequest;
import com.dimajix.flowman.kernel.proto.test.ListTestsRequest;


public final class SessionClient extends AbstractClient {
    private final Logger logger = LoggerFactory.getLogger(KernelClient.class);
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

    @Override
    public boolean isShutdown() {
        return channel.isTerminated();
    }

    @Override
    public boolean isTerminated() {
        return channel.isTerminated();
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
                Level level;
                switch(value.getLevel()) {
                    case TRACE:
                        level = Level.TRACE;
                        break;
                    case DEBUG:
                        level = Level.DEBUG;
                        break;
                    case INFO:
                        level = Level.INFO;
                        break;
                    case WARN:
                        level = Level.WARN;
                        break;
                    case ERROR:
                        level = Level.ERROR;
                        break;
                    default:
                        level = Level.ERROR;
                }
                val logger = value.getLogger();
                val message = value.getMessage();
                val ts = value.getTimestamp();
                val timestamp = ts.getSeconds() * 1000 + ts.getNanos() / 1000000;
                val throwable = value.hasException() ? ExceptionUtils.unwrap(value.getException()) : null;
                val event = new com.dimajix.common.logging.LogEvent(level, logger, message, timestamp, throwable);
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
        logger.info("Shutting down session '" + sessionId + "'");
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
    public StructType describeMapping(MappingOutputIdentifier mappingId, boolean useSpark) {
        val request = DescribeMappingRequest.newBuilder()
            .setSessionId(sessionId)
            .setMapping(mappingId.getMapping().toProto())
            .setOutput(mappingId.getOutput())
            .setUseSpark(useSpark)
            .build();
        val result = call(() -> blockingStub.describeMapping(request));
        val schema = result.getSchema();

        return StructType.ofProto(schema);
    }

    public void cacheMapping(MappingOutputIdentifier mappingId) {
        val request = CacheMappingRequest.newBuilder()
            .setSessionId(sessionId)
            .setMapping(mappingId.getMapping().toProto())
            .setOutput(mappingId.getOutput())
            .build();
        call(() -> blockingStub.cacheMapping(request));
    }

    public long countMapping(MappingOutputIdentifier mappingId) {
        val request = CountMappingRequest.newBuilder()
            .setSessionId(sessionId)
            .setMapping(mappingId.getMapping().toProto())
            .setOutput(mappingId.getOutput())
            .build();
        val result = call(() -> blockingStub.countMapping(request));

        return result.getNumRecords();
    }

    public DataFrame readMapping(MappingOutputIdentifier mappingId, List<String> columns, int maxRows) {
        val request = ReadMappingRequest.newBuilder()
            .setSessionId(sessionId)
            .setMapping(mappingId.getMapping().toProto())
            .setOutput(mappingId.getOutput())
            .addAllColumns(columns)
            .setMaxRows(maxRows)
            .build();
        val result = call(() -> blockingStub.readMapping(request));

        val df = result.getData();
        val rows = df.getRowsList().stream().map(Row::ofProto).collect(Collectors.toList());
        val schema = StructType.ofProto(df.getSchema());
        return new DataFrame(schema, rows);
    }

    public String explainMapping(MappingOutputIdentifier mappingId) {
        val request = ExplainMappingRequest.newBuilder()
            .setSessionId(sessionId)
            .setMapping(mappingId.getMapping().toProto())
            .setOutput(mappingId.getOutput())
            .build();
        val result = call(() -> blockingStub.explainMapping(request));

        return result.getPlan();
    }

    public void validateMapping(MappingIdentifier mappingId) {
        val request = ValidateMappingRequest.newBuilder()
            .setSessionId(sessionId)
            .setMapping(mappingId.toProto())
            .build();
        call(() -> blockingStub.validateMapping(request));
    }

    public Status saveMapping(MappingOutputIdentifier mappingId, String location, String format, Map<String,String> options) {
        val request = SaveMappingOutputRequest.newBuilder()
            .setSessionId(sessionId)
            .setMapping(mappingId.getMapping().toProto())
            .setOutput(mappingId.getOutput())
            .setFormat(format)
            .setLocation(location)
            .putAllOptions(options)
            .build();

        val result = call(() -> blockingStub.saveMappingOutput(request));
        return Status.ofProto(result.getStatus());
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
    public Status executeRelations(List<RelationIdentifier> relationIds, Phase phase, Map<String,String> partition, boolean force, boolean keepGoing, boolean dryRun) {
        val request = ExecuteRelationRequest.newBuilder()
            .setSessionId(sessionId)
            .addAllRelations(relationIds.stream().map(RelationIdentifier::toProto).collect(Collectors.toList()))
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
    public DataFrame readRelation(RelationIdentifier relationId, Map<String,String> partition, List<String> columns, int maxRows) {
        val request = ReadRelationRequest.newBuilder()
            .setSessionId(sessionId)
            .setRelation(relationId.toProto())
            .putAllPartition(partition)
            .addAllColumns(columns)
            .setMaxRows(maxRows)
            .build();
        val result = call(() -> blockingStub.readRelation(request));

        val df = result.getData();
        val rows = df.getRowsList().stream().map(Row::ofProto).collect(Collectors.toList());
        val schema = StructType.ofProto(df.getSchema());
        return new DataFrame(schema, rows);
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
    public Status executeTests(List<TestIdentifier> testIds, boolean keepGoing, boolean dryRun, int parallelism) {
        val request = ExecuteTestRequest.newBuilder()
            .setSessionId(sessionId)
            .addAllTests(testIds.stream().map(TestIdentifier::toProto).collect(Collectors.toList()))
            .setKeepGoing(keepGoing)
            .setDryRun(dryRun)
            .setParallelism(parallelism)
            .build();

        val result = call(() -> blockingStub.executeTest(request));
        val status = result.getStatus();

        return Status.ofProto(status);
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

    public void generateDocumentation(JobIdentifier jobId, Map<String,String> args) {
        val request = GenerateDocumentationRequest.newBuilder()
            .setSessionId(sessionId)
            .setJob(jobId.toProto())
            .putAllArguments(args)
            .build();
        call(() -> blockingStub.generateDocumentation(request));
    }

    public Status executeProject(List<Phase> lifecycle, Map<String,String> args, List<String> targets,List<String> dirtyTargets,boolean force, boolean keepGoing, boolean dryRun,int parallelism) {
        val request = ExecuteProjectRequest.newBuilder()
            .setSessionId(sessionId)
            .addAllPhases(lifecycle.stream().map(Phase::toProto).collect(Collectors.toList()))
            .putAllArguments(args)
            .addAllTargets(targets)
            .addAllDirtyTargets(dirtyTargets)
            .setForce(force)
            .setKeepGoing(keepGoing)
            .setDryRun(dryRun)
            .setParallelism(parallelism)
            .build();

        val result = call(() -> blockingStub.executeProject(request));
        val status = result.getStatus();

        return Status.ofProto(status);
    }

    public DataFrame executeSql(String statement, int maxRows) {
        val request = ExecuteSqlRequest.newBuilder()
            .setSessionId(sessionId)
            .setStatement(statement)
            .setMaxRows(maxRows)
            .build();
        val result = call(() -> blockingStub.executeSql(request));

        val df = result.getData();
        val rows = df.getRowsList().stream().map(Row::ofProto).collect(Collectors.toList());
        val schema = StructType.ofProto(df.getSchema());
        return new DataFrame(schema, rows);
    }

    public String evaluateExpression(String expression) {
        val request = EvaluateExpressionRequest.newBuilder()
            .setSessionId(sessionId)
            .setExpression(expression)
            .build();

        val result = call(() -> blockingStub.evaluateExpression(request));

        return result.getResult();
    }
}
