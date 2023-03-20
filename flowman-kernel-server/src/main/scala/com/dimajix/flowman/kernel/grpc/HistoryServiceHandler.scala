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

import scala.collection.JavaConverters._

import io.grpc.stub.StreamObserver
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.history.TargetQuery
import com.dimajix.flowman.kernel.RpcConverters.toModel
import com.dimajix.flowman.kernel.RpcConverters.toProto
import com.dimajix.flowman.kernel.proto.history.FindJobMetricsRequest
import com.dimajix.flowman.kernel.proto.history.FindJobMetricsResponse
import com.dimajix.flowman.kernel.proto.history.FindJobsRequest
import com.dimajix.flowman.kernel.proto.history.FindJobsResponse
import com.dimajix.flowman.kernel.proto.history.FindTargetsRequest
import com.dimajix.flowman.kernel.proto.history.FindTargetsResponse
import com.dimajix.flowman.kernel.proto.history.GetJobMetricsRequest
import com.dimajix.flowman.kernel.proto.history.GetJobMetricsResponse
import com.dimajix.flowman.kernel.proto.history.HistoryServiceGrpc
import com.dimajix.flowman.kernel.proto.history.JobHistoryDetails
import com.dimajix.flowman.kernel.proto.history.JobHistoryQuery
import com.dimajix.flowman.kernel.proto.history.TargetHistoryDetails
import com.dimajix.flowman.kernel.proto.history.TargetHistoryQuery
import com.dimajix.flowman.kernel.service.SessionManager


final class HistoryServiceHandler(
    sessionManager: SessionManager
) extends HistoryServiceGrpc.HistoryServiceImplBase with ServiceHandler {
    override protected val logger:Logger = LoggerFactory.getLogger(classOf[HistoryServiceHandler])


    /**
     */
    override def findTargets(request: FindTargetsRequest, responseObserver: StreamObserver[FindTargetsResponse]): Unit = {
        respondTo("findTargets", responseObserver) {
            val history = sessionManager.rootSession.history
            val query = buildQuery(request.getQuery)
            val order = request.getOrderList.asScala.map(toModel)
            val result = history.findTargets(query, order, request.getMaxResults)

            val details = result.map { t =>
                val builder = TargetHistoryDetails.newBuilder()
                    .setId(t.id)
                    .setNamespace(t.namespace)
                    .setProject(t.project)
                    .setVersion(t.version)
                    .setTarget(t.target)
                    .setPhase(toProto(t.phase))
                    .setStatus(toProto(t.status))
                    .putAllPartitions(t.partitions.asJava)
                t.startDateTime.foreach(dt => builder.setStartDateTime(toProto(dt)))
                t.endDateTime.foreach(dt => builder.setEndDateTime(toProto(dt)))
                t.jobId.foreach(builder.setJobId)
                t.error.foreach(builder.setError)
                builder.build()
            }

            FindTargetsResponse.newBuilder()
                .addAllTargets(details.asJava)
                .build()
        }
    }

    override def findJobs(request: FindJobsRequest, responseObserver: StreamObserver[FindJobsResponse]): Unit = {
        respondTo("findJobs", responseObserver) {
            val history = sessionManager.rootSession.history
            val query = buildQuery(request.getQuery)
            val order = request.getOrderList.asScala.map(toModel)
            val result = history.findJobs(query, order, request.getMaxResults)

            val details = result.map { t =>
                val builder = JobHistoryDetails.newBuilder()
                    .setId(t.id)
                    .setNamespace(t.namespace)
                    .setProject(t.project)
                    .setVersion(t.version)
                    .setJob(t.job)
                    .setPhase(toProto(t.phase))
                    .setStatus(toProto(t.status))
                    .putAllArguments(t.args.asJava)
                t.startDateTime.foreach(dt => builder.setStartDateTime(toProto(dt)))
                t.endDateTime.foreach(dt => builder.setEndDateTime(toProto(dt)))
                t.error.foreach(builder.setError)
                builder.build()
            }

            FindJobsResponse.newBuilder()
                .addAllJobs(details.asJava)
                .build()
        }
    }

    /**
      */
    override def getJobMetrics(request: GetJobMetricsRequest, responseObserver: StreamObserver[GetJobMetricsResponse]): Unit = {
        respondTo("getJobMetrics", responseObserver) {
            val history = sessionManager.rootSession.history
            val result = history.getJobMetrics(request.getJobId)

            val measurements = result.map(toProto)

            GetJobMetricsResponse.newBuilder()
                .addAllMeasurements(measurements.asJava)
                .build()
        }
    }
    /**
      */
    override def findJobMetrics(request: FindJobMetricsRequest, responseObserver: StreamObserver[FindJobMetricsResponse]): Unit = {
        respondTo("findJobMetrics", responseObserver) {
            val history = sessionManager.rootSession.history
            val query = buildQuery(request.getQuery)
            val result = history.findJobMetrics(query, request.getGroupingsList.asScala)

            val series = result.map(toProto)

            FindJobMetricsResponse.newBuilder()
                .addAllMetrics(series.asJava)
                .build()
        }
    }

    private def buildQuery(pquery:TargetHistoryQuery) : TargetQuery = {
        TargetQuery(
            id = pquery.getIdList.asScala,
            namespace = pquery.getNamespaceList.asScala,
            project = pquery.getProjectList.asScala,
            target = pquery.getTargetList.asScala,
            status = pquery.getStatusList.asScala.map(toModel),
            phase = pquery.getPhaseList.asScala.map(toModel),
            job = pquery.getJobList.asScala,
            jobId = pquery.getJobIdList.asScala,
            from = if (pquery.hasFrom) Some(toModel(pquery.getFrom)) else None,
            to = if (pquery.hasUntil) Some(toModel(pquery.getUntil)) else None,
            partitions = pquery.getPartitionsMap.asScala.toMap
        )
    }
    private def buildQuery(pquery: JobHistoryQuery): JobQuery = {
        JobQuery(
            id = pquery.getIdList.asScala,
            namespace = pquery.getNamespaceList.asScala,
            project = pquery.getProjectList.asScala,
            status = pquery.getStatusList.asScala.map(toModel),
            phase = pquery.getPhaseList.asScala.map(toModel),
            job = pquery.getJobList.asScala,
            from = if (pquery.hasFrom) Some(toModel(pquery.getFrom)) else None,
            to = if (pquery.hasUntil) Some(toModel(pquery.getUntil)) else None,
            args = pquery.getArgumentsMap.asScala.toMap
        )
    }
}
