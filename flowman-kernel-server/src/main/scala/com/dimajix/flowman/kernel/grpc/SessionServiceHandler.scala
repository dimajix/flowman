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

package com.dimajix.flowman.kernel.grpc

import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.grpc.ExceptionUtils
import com.dimajix.flowman.kernel.RpcConverters._
import com.dimajix.flowman.kernel.grpc.ClientIdExtractor.CLIENT_ID
import com.dimajix.flowman.kernel.proto.DataFrame
import com.dimajix.flowman.kernel.proto.JobContext
import com.dimajix.flowman.kernel.proto.JobIdentifier
import com.dimajix.flowman.kernel.proto.LogEvent
import com.dimajix.flowman.kernel.proto.LogLevel
import com.dimajix.flowman.kernel.proto.MappingIdentifier
import com.dimajix.flowman.kernel.proto.RelationIdentifier
import com.dimajix.flowman.kernel.proto.Session
import com.dimajix.flowman.kernel.proto.TargetIdentifier
import com.dimajix.flowman.kernel.proto.TestContext
import com.dimajix.flowman.kernel.proto.TestIdentifier
import com.dimajix.flowman.kernel.proto.Timestamp
import com.dimajix.flowman.kernel.proto.job._
import com.dimajix.flowman.kernel.proto.mapping._
import com.dimajix.flowman.kernel.proto.project._
import com.dimajix.flowman.kernel.proto.relation._
import com.dimajix.flowman.kernel.proto.session._
import com.dimajix.flowman.kernel.proto.target._
import com.dimajix.flowman.kernel.proto.test._
import com.dimajix.flowman.kernel.service.SessionManager
import com.dimajix.flowman.kernel.service.SessionService
import com.dimajix.flowman.kernel.service.WorkspaceManager
import com.dimajix.flowman.model
import com.dimajix.flowman.storage.Workspace
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StructType


final class SessionServiceHandler(
    sessionManager: SessionManager,
    workspaceManager: WorkspaceManager
) extends SessionServiceGrpc.SessionServiceImplBase with ServiceHandler with ClientWatcher {
    override protected val logger:Logger = LoggerFactory.getLogger(classOf[SessionServiceHandler])


    override def clientConnected(clientId: UUID): Unit = {

    }

    override def clientDisconnected(clientId: UUID): Unit = {
        sessionManager.removeClientSessions(clientId)
    }

    /**
     */
    override def createSession(request: CreateSessionRequest, responseObserver: StreamObserver[CreateSessionResponse]): Unit = {
        respondTo("createSession", responseObserver) {
            val clientId = CLIENT_ID.get()
            val store =
                if (request.hasWorkspace()) {
                    val workspaceName = request.getWorkspace;
                    workspaceManager.getWorkspace(workspaceName)
                }
                else {
                    sessionManager.rootSession.store
                }

            val session = {
                if (request.hasProjectLocation) {
                    val file = store match {
                        case ws:Workspace => ws.root / request.getProjectLocation
                        case _ =>
                            sessionManager.rootSession.fs.file(request.getProjectLocation)
                    }

                    sessionManager.createSession(store, file, clientId)
                }
                else if (request.hasProjectName) {
                    sessionManager.createSession(store, request.getProjectName, clientId)
                }
                else {
                    throw new IllegalArgumentException("Either project name or project location is required for creating a new session")
                }
            }

            val sessionDetails = createSessionDetails(session)
            CreateSessionResponse.newBuilder()
                .setSession(sessionDetails)
                .build()
        }
    }

    /**
     */
    override def listSessions(request: ListSessionsRequest, responseObserver: StreamObserver[ListSessionsResponse]): Unit = {
        respondTo("listSessions", responseObserver) {
            val sessions = sessionManager.list()
                .map(s => Session.newBuilder()
                    .setId(s.id)
                    .setName(s.name)
                    .setWorkspace(s.store.name)
                    .setNamespace(s.namespace.name)
                    .setProject(s.project.name)
                    .build()
                )

            ListSessionsResponse.newBuilder()
                .addAllSessions(sessions.asJava)
                .build()
        }
    }

    /**
     */
    override def getSession(request: GetSessionRequest, responseObserver: StreamObserver[GetSessionResponse]): Unit = {
        respondTo("getSession", responseObserver) {
            val session = getSession(request.getSessionId)
            val sessionDetails = createSessionDetails(session)
            GetSessionResponse.newBuilder()
                .setSession(sessionDetails)
                .build()
        }
    }

    /**
     */
    override def deleteSession(request: DeleteSessionRequest, responseObserver: StreamObserver[DeleteSessionResponse]): Unit = {
        respondTo("deleteSession", responseObserver) {
            val session = getSession(request.getSessionId)
            session.close()
            DeleteSessionResponse.newBuilder().build()
        }
    }

    /**
     */
    override def enterContext(request: EnterContextRequest, responseObserver: StreamObserver[EnterContextResponse]): Unit = {
        respondTo("enterContext", responseObserver) {
            val session = getSession(request.getSessionId)

            if (request.hasJob) {
                val jobContext = request.getJob
                val job = session.getJob(toModel(jobContext.getJob))
                session.enterJob(job, jobContext.getArgumentsMap.asScala.toMap)
            }
            else if (request.hasTest) {
                val testContext = request.getTest
                val test = session.getTest(toModel(testContext.getTest))
                session.enterTest(test)
            }
            else {
                throw new IllegalArgumentException("Neither job nor test was specified for entering a context")
            }

            EnterContextResponse.newBuilder().build()
        }
    }

    /**
     */
    override def leaveContext(request: LeaveContextRequest, responseObserver: StreamObserver[LeaveContextResponse]): Unit = {
        respondTo("leaveContext", responseObserver) {
            val session = getSession(request.getSessionId)
            session.leaveJob()
            session.leaveTest()

            LeaveContextResponse.newBuilder().build()
        }
    }

    /**
     */
    override def getContext(request: GetContextRequest, responseObserver: StreamObserver[GetContextResponse]): Unit = {
        respondTo("getContext", responseObserver) {
            val session = getSession(request.getSessionId)

            val response = GetContextResponse.newBuilder()
            session.test.foreach { t =>
                val tc = TestContext.newBuilder()
                    .setTest(toProto(t.identifier))
                response.setTest(tc.build())
            }
            session.job.foreach { t =>
                val tc = JobContext.newBuilder()
                    .setJob(toProto(t.identifier))
                    .putAllArguments(session.jobArgs.asJava)
                response.setJob(tc.build())
            }
            response.setProject(session.project.name)

            response.build()
        }
    }

    /**
     * <pre>
     * Mappings stuff
     * </pre>
     */
    override def listMappings(request: ListMappingsRequest, responseObserver: StreamObserver[ListMappingsResponse]): Unit = {
        respondTo("listMappings", responseObserver) {
            val session = getSession(request.getSessionId)
            val mappings = session.listMappings().map(toProto)

            ListMappingsResponse.newBuilder()
                .addAllMappings(mappings.asJava)
                .build()
        }
    }

    /**
     */
    override def getMapping(request: GetMappingRequest, responseObserver: StreamObserver[GetMappingResponse]): Unit = {
        respondTo("getMapping", responseObserver) {
            val session = getSession(request.getSessionId)
            val mapping = getMapping(session, request.getMapping)

            val inputs = mapping.inputs.toSeq.map(toProto)
            val requires = mapping.requires.toSeq.map(toProto)
            val details = MappingDetails.newBuilder()
                .setName(mapping.name)
                .setKind(mapping.kind)
                .addAllOutputs(mapping.outputs.asJava)
                .addAllInputs(inputs.asJava)
                .setCache(mapping.cache.toString())
                .setBroadcast(mapping.broadcast)
                .setCheckpoint(mapping.checkpoint)
                .addAllRequired(requires.asJava)
            mapping.project.foreach(p => details.setProject(p.name))

            GetMappingResponse.newBuilder()
                .setMapping(details)
                .build()
        }
    }

    /**
     */
    override def readMapping(request: ReadMappingRequest, responseObserver: StreamObserver[ReadMappingResponse]): Unit = {
        respondTo("readMapping", responseObserver) {
            val session = getSession(request.getSessionId)
            val mapping = getMapping(session, request.getMapping)
            val output = if (request.getOutput.nonEmpty) request.getOutput else "main"
            val columns = request.getColumnsList.asScala
            val execution = session.execution

            val df = execution.instantiate(mapping, output)
            val data = formatDataFrame(df, columns, request.getMaxRows)
            ReadMappingResponse.newBuilder()
                .setData(data)
                .build()
        }
    }
    /**
     */
    override def describeMapping(request: DescribeMappingRequest, responseObserver: StreamObserver[DescribeMappingResponse]): Unit = {
        respondTo("describeMapping", responseObserver) {
            val session = getSession(request.getSessionId)
            val mapping = getMapping(session, request.getMapping)
            val output = if (request.getOutput.nonEmpty) request.getOutput else "main"
            val execution = session.execution

            val schema = if (request.getUseSpark) {
                val df = execution.instantiate(mapping, output)
                StructType.of(df.schema)
            }
            else {
                execution.describe(mapping, output)
            }

            DescribeMappingResponse.newBuilder()
                .setSchema(toProto(schema))
                .build()
        }
    }

    /**
     */
    override def validateMapping(request: ValidateMappingRequest, responseObserver: StreamObserver[ValidateMappingResponse]): Unit = {
        respondTo("validateMapping", responseObserver) {
            val session = getSession(request.getSessionId)
            val mapping = getMapping(session, request.getMapping)
            val executor = session.execution

            executor.instantiate(mapping)

            ValidateMappingResponse.newBuilder()
                .build()
        }
    }

    /**
     */
    override def saveMappingOutput(request: SaveMappingOutputRequest, responseObserver: StreamObserver[SaveMappingOutputResponse]): Unit = super.saveMappingOutput(request, responseObserver)

    /**
     */
    override def countMapping(request: CountMappingRequest, responseObserver: StreamObserver[CountMappingResponse]): Unit = super.countMapping(request, responseObserver)

    /**
     */
    override def cacheMapping(request: CacheMappingRequest, responseObserver: StreamObserver[CacheMappingResponse]): Unit = super.cacheMapping(request, responseObserver)

    /**
     */
    override def explainMapping(request: ExplainMappingRequest, responseObserver: StreamObserver[ExplainMappingResponse]): Unit = super.explainMapping(request, responseObserver)

    /**
     * <pre>
     * Relations stuff
     * </pre>
     */
    override def listRelations(request: ListRelationsRequest, responseObserver: StreamObserver[ListRelationsResponse]): Unit = {
        respondTo("listRelations", responseObserver) {
            val session = getSession(request.getSessionId)
            val relations = session.listRelations().map(toProto)

            ListRelationsResponse.newBuilder()
                .addAllRelations(relations.asJava)
                .build()
        }
    }

    /**
     */
    override def getRelation(request: GetRelationRequest, responseObserver: StreamObserver[GetRelationResponse]): Unit = {
        respondTo("getRelation", responseObserver) {
            val session = getSession(request.getSessionId)
            val relation = getRelation(session, request.getRelation)

            val details = RelationDetails.newBuilder()
                .setName(relation.name)
                .setKind(relation.kind)
                .addAllRequiredByCreate(relation.requires(Operation.CREATE).toSeq.map(toProto).asJava)
                .addAllRequiredByWrite(relation.requires(Operation.WRITE).toSeq.map(toProto).asJava)
                .addAllRequiredByRead(relation.requires(Operation.READ).toSeq.map(toProto).asJava)
                .addAllProvidedByCreate(relation.provides(Operation.CREATE).toSeq.map(toProto).asJava)
                .addAllProvidedByWrite(relation.provides(Operation.WRITE).toSeq.map(toProto).asJava)
                .addAllProvidedByRead(relation.provides(Operation.READ).toSeq.map(toProto).asJava)
            relation.project.foreach(p => details.setProject(p.name))

            GetRelationResponse.newBuilder()
                .setRelation(details)
                .build()
        }
    }

    /**
     */
    override def readRelation(request: ReadRelationRequest, responseObserver: StreamObserver[ReadRelationResponse]): Unit = {
        respondTo("readRelation", responseObserver) {
            val session = getSession(request.getSessionId)
            val relation = getRelation(session, request.getRelation)
            val columns = request.getColumnsList.asScala
            val execution = session.execution
            val partition = request.getPartitionMap.asScala.toMap.map { case(k,v) => (k,SingleValue(v)) }

            val df = relation.read(execution, partition)
            val data = formatDataFrame(df, columns, request.getMaxRows)
            ReadRelationResponse.newBuilder()
                .setData(data)
                .build()
        }
    }

    /**
     */
    override def executeRelation(request: ExecuteRelationRequest, responseObserver: StreamObserver[ExecuteRelationResponse]): Unit = {
        respondTo("executeRelation", responseObserver) {
            val session = getSession(request.getSessionId)
            val context = session.context
            val relations = request.getRelationsList
                .asScala
                .map(t => context.getRelation(toModel(t)))
            val phase = toModel(request.getPhase)
            val partition = request.getPartitionMap.asScala.toMap
            val force = request.getForce
            val keepGoing = request.getKeepGoing
            val dryRun = request.getDryRun

            val status = session.executeRelations(relations, phase, partition, force, keepGoing, dryRun)

            ExecuteRelationResponse.newBuilder()
                .setStatus(toProto(status))
                .build()
        }
    }
    /**
     */
    override def describeRelation(request: DescribeRelationRequest, responseObserver: StreamObserver[DescribeRelationResponse]): Unit = {
        respondTo("describeRelation", responseObserver) {
            val session = getSession(request.getSessionId)
            val relation = getRelation(session, request.getRelation)
            val partition = request.getPartitionMap().asScala.toMap.map { case(k,v) => k -> SingleValue(v) }
            val execution = session.execution

            val schema = if (request.getUseSpark) {
                val df = relation.read(session.execution, partition)
                StructType.of(df.schema)
            }
            else {
                relation.describe(execution, partition)
            }

            DescribeRelationResponse.newBuilder()
                .setSchema(toProto(schema))
                .build()
        }
    }

    /**
     * <pre>
     * Targets stuff
     * </pre>
     */
    override def listTargets(request: ListTargetsRequest, responseObserver: StreamObserver[ListTargetsResponse]): Unit = {
        respondTo("listTargets", responseObserver) {
            val session = getSession(request.getSessionId)
            val targets = session.listTargets().map(toProto)

            ListTargetsResponse.newBuilder()
                .addAllTargets(targets.asJava)
                .build()
        }
    }

    /**
     */
    override def getTarget(request: GetTargetRequest, responseObserver: StreamObserver[GetTargetResponse]): Unit = {
        respondTo("getTarget", responseObserver) {
            val session = getSession(request.getSessionId)
            val target = getTarget(session, request.getTarget)

            val details = TargetDetails.newBuilder()
                .setName(target.name)
                .setKind(target.kind)
                .addAllRequiredByValidate(target.requires(Phase.VALIDATE).toSeq.map(toProto).asJava)
                .addAllRequiredByCreate(target.requires(Phase.CREATE).toSeq.map(toProto).asJava)
                .addAllRequiredByBuild(target.requires(Phase.BUILD).toSeq.map(toProto).asJava)
                .addAllRequiredByVerify(target.requires(Phase.VERIFY).toSeq.map(toProto).asJava)
                .addAllRequiredByTruncate(target.requires(Phase.TRUNCATE).toSeq.map(toProto).asJava)
                .addAllRequiredByDestroy(target.requires(Phase.DESTROY).toSeq.map(toProto).asJava)
                .addAllProvidedByValidate(target.provides(Phase.VALIDATE).toSeq.map(toProto).asJava)
                .addAllProvidedByCreate(target.provides(Phase.CREATE).toSeq.map(toProto).asJava)
                .addAllProvidedByBuild(target.provides(Phase.BUILD).toSeq.map(toProto).asJava)
                .addAllProvidedByVerify(target.provides(Phase.VERIFY).toSeq.map(toProto).asJava)
                .addAllProvidedByTruncate(target.provides(Phase.TRUNCATE).toSeq.map(toProto).asJava)
                .addAllProvidedByDestroy(target.provides(Phase.DESTROY).toSeq.map(toProto).asJava)
            target.project.foreach(p => details.setProject(p.name))

            GetTargetResponse.newBuilder()
                .setTarget(details)
                .build()
        }
    }

    /**
     */
    override def executeTarget(request: ExecuteTargetRequest, responseObserver: StreamObserver[ExecuteTargetResponse]): Unit = {
        respondTo ("executeTarget", responseObserver) {
            val session = getSession (request.getSessionId)
            val context = session.context
            val allTargets = request.getTargetsList
                .asScala
                .map(t => context.getTarget(toModel(t)))

            val lifecycle = request.getPhasesList ().asScala.map (toModel)
            val force = request.getForce
            val keepGoing = request.getKeepGoing
            val dryRun = request.getDryRun

            val status = session.executeTargets (allTargets, lifecycle, force, keepGoing, dryRun)

            ExecuteTargetResponse.newBuilder()
                .setStatus(toProto(status))
                .build();
        }
    }

    /**
     * <pre>
     * Tests stuff
     * </pre>
     */
    override def listTests(request: ListTestsRequest, responseObserver: StreamObserver[ListTestsResponse]): Unit = {
        respondTo("listTests", responseObserver) {
            val session = getSession(request.getSessionId)
            val tests = session.listTests().map(toProto)

            ListTestsResponse.newBuilder()
                .addAllTests(tests.asJava)
                .build()
        }
    }

    /**
     */
    override def getTest(request: GetTestRequest, responseObserver: StreamObserver[GetTestResponse]): Unit = super.getTest(request, responseObserver)

    /**
     */
    override def executeTest(request: ExecuteTestRequest, responseObserver: StreamObserver[ExecuteTestResponse]): Unit = super.executeTest(request, responseObserver)

    /**
     * <pre>
     * Job stuff
     * </pre>
     */
    override def listJobs(request: ListJobsRequest, responseObserver: StreamObserver[ListJobsResponse]): Unit = {
        respondTo("listJobs", responseObserver) {
            val session = getSession(request.getSessionId)
            val jobs = session.listJobs().map(toProto)

            ListJobsResponse.newBuilder()
                .addAllJobs(jobs.asJava)
                .build()
        }
    }

    /**
     */
    override def getJob(request: GetJobRequest, responseObserver: StreamObserver[GetJobResponse]): Unit = {
        respondTo("getJob", responseObserver) {
            val session = getSession(request.getSessionId)
            val job = session.getJob(toModel(request.getJob))

            val details = JobDetails.newBuilder()
                .setName(job.name)
                .addAllTargets(job.targets.map(toProto).asJava)
                .addAllParameters(job.parameters.map { p =>
                    val jp = JobParameter.newBuilder()
                        .setName(p.name)
                        .setType(p.ftype.typeName)
                    p.granularity.foreach(jp.setGranularity)
                    p.default.foreach(d => jp.setDefault(d.toString))
                    p.description.foreach(jp.setDescription)
                    jp.build()
                }.asJava)
            job.project.foreach(p => details.setProject(p.name))
            job.description.foreach(details.setDescription)

            GetJobResponse.newBuilder()
                .setJob(details.build())
                .build()
        }
    }

    /**
     */
    override def executeJob(request: ExecuteJobRequest, responseObserver: StreamObserver[ExecuteJobResponse]): Unit = {
        respondTo("executeJob", responseObserver) {
            val session = getSession(request.getSessionId)
            val job = session.getJob(toModel(request.getJob))

            val lifecycle = request.getPhasesList().asScala.map(toModel)
            val force = request.getForce
            val keepGoing = request.getKeepGoing
            val dryRun = request.getDryRun
            val parallelism = request.getParallelism
            val args = request.getArgumentsMap
            val targets = request.getTargetsList
            val dirtyTargets = request.getDirtyTargetsList()

            val status = session.executeJob(job, lifecycle, args.asScala.toMap, targets.asScala, dirtyTargets.asScala, force, keepGoing, dryRun, parallelism)

            ExecuteJobResponse.newBuilder()
                .setStatus(toProto(status))
                .build();
        }
    }

    /**
     * <pre>
     * Project stuff
     * </pre>
     */
    override def getProject(request: GetProjectRequest, responseObserver: StreamObserver[GetProjectResponse]): Unit = {
        respondTo("getProject", responseObserver) {
            val session = getSession(request.getSessionId)
            val project = session.project

            val details = ProjectDetails.newBuilder()
                .setName(project.name)
                .addAllProfiles(project.profiles.keys.asJava)
                .putAllEnvironment(project.environment.map { case (k, v) => k -> v }.asJava)
                .putAllConfig(project.config.asJava)
                .addAllConnections(project.connections.keys.toSeq.asJava)
                .addAllJobs(project.jobs.keys.toSeq.asJava)
                .addAllTests(project.tests.keys.toSeq.asJava)
                .addAllTargets(project.targets.keys.toSeq.asJava)
                .addAllMappings(project.mappings.keys.toSeq.asJava)
                .addAllRelations(project.relations.keys.toSeq.asJava)
                .addAllProfiles(project.profiles.keys.toSeq.asJava)
            project.version.foreach(details.setVersion)
            project.description.foreach(details.setDescription)
            project.basedir.map(_.absolute.toString).foreach(details.setBasedir)
            project.filename.map(_.absolute.toString).foreach(details.setFilename)

            GetProjectResponse.newBuilder()
                .setProject(details.build())
                .build()
        }
    }

    /**
     */
    override def executeProject(request: ExecuteProjectRequest, responseObserver: StreamObserver[ExecuteProjectResponse]): Unit = super.executeProject(request, responseObserver)

    /**
     * <pre>
     * Generic stuff
     * </pre>
     */
    override def executeSql(request: ExecuteSqlRequest, responseObserver: StreamObserver[ExecuteSqlResponse]): Unit = super.executeSql(request, responseObserver)


    /**
     */
    override def subscribeLog(request: SubscribeLogRequest, responseObserver: StreamObserver[LogEvent]): Unit = {
        try {
            val session = getSession(request.getSessionId)
            session.loggerFactory.addSink({ev =>
                val level = ev.getLevel match {
                    case org.slf4j.event.Level.TRACE => LogLevel.TRACE
                    case org.slf4j.event.Level.DEBUG => LogLevel.DEBUG
                    case org.slf4j.event.Level.INFO => LogLevel.INFO
                    case org.slf4j.event.Level.WARN => LogLevel.WARN
                    case org.slf4j.event.Level.ERROR => LogLevel.ERROR
                }
                val ts = ev.getTimeStamp
                val result = LogEvent.newBuilder()
                    .setLogger(ev.getLoggerName)
                    .setLevel(level)
                    .setMessage(ev.getMessage)
                    .setTimestamp(Timestamp.newBuilder().setSeconds(ts / 1000).setNanos((1000000 * (ts % 1000)).toInt))
                val ex = ev.getThrowable
                if (ex != null)
                    result.setException(ExceptionUtils.wrap(ex, false))
                responseObserver.onNext(result.build())
            })
        }
        catch {
            case e@(_: StatusException | _: StatusRuntimeException) =>
                logger.error(s"Exception during execution of RPC call ${getClass.getSimpleName}.subscribeLog:\n  ${reasons(e)}")
                responseObserver.onError(e)
            case NonFatal(t) =>
                logger.error(s"Exception during execution of RPC call ${getClass.getSimpleName}.subscribeLog:\n  ${reasons(t)}")
                val e = ExceptionUtils.asStatusException(Status.INTERNAL, t, true)
                responseObserver.onError(e)
        }
    }

    private def formatDataFrame(df:org.apache.spark.sql.DataFrame, columns:Seq[String], maxRows:Int) : DataFrame = {
        df.limit(maxRows)

        val dfOut =
            if (columns.nonEmpty)
                df.select(columns.map(c => df(c)): _*)
            else
                df

        val schema = StructType.of(dfOut.schema)
        val rows = dfOut.collect()
        DataFrame.newBuilder()
            .setSchema(toProto(schema))
            .addAllRows(rows.toSeq.map(toProto).asJava)
            .build()
    }
    private def getMapping(session:SessionService, mappingIdentifier: MappingIdentifier) = {
        val mappingId = model.MappingIdentifier(mappingIdentifier.getName, Some(mappingIdentifier.getProject).filter(_.nonEmpty))
        session.getMapping(mappingId)
    }

    private def getRelation(session: SessionService, relationIdentifier: RelationIdentifier) = {
        val relationId = model.RelationIdentifier(relationIdentifier.getName, Some(relationIdentifier.getProject).filter(_.nonEmpty))
        session.getRelation(relationId)
    }

    private def getTarget(session: SessionService, targetIdentifier: TargetIdentifier) = {
        val targetId = model.TargetIdentifier(targetIdentifier.getName, Some(targetIdentifier.getProject).filter(_.nonEmpty))
        session.getTarget(targetId)
    }

    private def getTest(session: SessionService, testIdentifier: TestIdentifier) = {
        val testId = model.TestIdentifier(testIdentifier.getName, Some(testIdentifier.getProject).filter(_.nonEmpty))
        session.getTest(testId)
    }

    private def getJob(session: SessionService, jobIdentifier: JobIdentifier) = {
        val jobId = model.JobIdentifier(jobIdentifier.getName, Some(jobIdentifier.getProject).filter(_.nonEmpty))
        session.getJob(jobId)
    }

    private def getSession(sessionId: String) : SessionService = {
        sessionManager.getSession(sessionId)
            .getOrElse(throw new NoSuchElementException(s"No Session $sessionId"))
    }
    private def createSessionDetails(session: SessionService): SessionDetails = {
        val project = session.project
        val context = session.context

        val sessionDetailsBuilder = SessionDetails.newBuilder()
            .setId(session.id)
            .setName(session.name)
            .setWorkspace(session.store.name)
            .setNamespace(session.namespace.name)
            .setProject(project.name)
            .addAllProfiles(project.profiles.keys.asJava)
            .putAllEnvironment(context.environment.toMap.map { case (k, v) => k -> v.toString }.asJava)
            .putAllConfig(context.config.toMap.asJava)
            .putAllFlowmanConfig(context.flowmanConf.getAll.toMap.asJava)
            .putAllSparkConfig(context.sparkConf.getAll.toMap.asJava)
            .putAllHadoopConfig(context.hadoopConf.asScala.map(k => k.getKey -> k.getValue).toMap.asJava)
        project.version.foreach(sessionDetailsBuilder.setProjectVersion)
        project.description.foreach(sessionDetailsBuilder.setProjectDescription)
        project.basedir.map(_.absolute.toString).foreach(sessionDetailsBuilder.setProjectBasedir)
        project.filename.map(_.absolute.toString).foreach(sessionDetailsBuilder.setProjectFilename)
        sessionDetailsBuilder.build()
    }
}
