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

package com.dimajix.flowman.kernel.grpc

import scala.collection.JavaConverters._

import io.grpc.stub.StreamObserver

import com.dimajix.flowman.grpc.GrpcService
import com.dimajix.flowman.kernel.RpcUtils.respondTo
import com.dimajix.flowman.kernel.RpcUtils.streamRequests
import com.dimajix.flowman.kernel.proto.Workspace
import com.dimajix.flowman.kernel.proto.workspace.CleanWorkspaceRequest
import com.dimajix.flowman.kernel.proto.workspace.CleanWorkspaceResponse
import com.dimajix.flowman.kernel.proto.workspace.CreateWorkspaceRequest
import com.dimajix.flowman.kernel.proto.workspace.CreateWorkspaceResponse
import com.dimajix.flowman.kernel.proto.workspace.DeleteWorkspaceRequest
import com.dimajix.flowman.kernel.proto.workspace.DeleteWorkspaceResponse
import com.dimajix.flowman.kernel.proto.workspace.GetWorkspaceRequest
import com.dimajix.flowman.kernel.proto.workspace.GetWorkspaceResponse
import com.dimajix.flowman.kernel.proto.workspace.ListWorkspacesRequest
import com.dimajix.flowman.kernel.proto.workspace.ListWorkspacesResponse
import com.dimajix.flowman.kernel.proto.workspace.UploadFilesRequest
import com.dimajix.flowman.kernel.proto.workspace.UploadFilesResponse
import com.dimajix.flowman.kernel.proto.workspace.WorkspaceDetails
import com.dimajix.flowman.kernel.proto.workspace.WorkspaceServiceGrpc
import com.dimajix.flowman.kernel.service.WorkspaceManager


final class WorkspaceServiceHandler(manager:WorkspaceManager) extends WorkspaceServiceGrpc.WorkspaceServiceImplBase with GrpcService {
    /**
     */
    override def createWorkspace(request: CreateWorkspaceRequest, responseObserver: StreamObserver[CreateWorkspaceResponse]): Unit = {
        respondTo(responseObserver) {
            val name = request.getName
            val ws = {
                if (request.getIfNotExists && manager.list().exists(_.name == name)) {
                    manager.getWorkspace(name)
                }
                else {
                    manager.createWorkspace(name)
                }
            };
            CreateWorkspaceResponse.newBuilder()
                .setWorkspace(WorkspaceDetails.newBuilder()
                    .setName(ws.name)
                    .addAllProjects(ws.listProjects().map(_.name).asJava)
                    .build()
                )
                .build()
        }
    }

    /**
     */
    override def listWorkspaces(request: ListWorkspacesRequest, responseObserver: StreamObserver[ListWorkspacesResponse]): Unit = {
        respondTo(responseObserver) {
            val list = manager.list()
            val ws = list.map(ws => Workspace.newBuilder().setName(ws.name).build())
            ListWorkspacesResponse.newBuilder()
                .addAllWorkspaces(ws.asJava)
                .build()
        }
    }

    /**
     */
    override def getWorkspace(request: GetWorkspaceRequest, responseObserver: StreamObserver[GetWorkspaceResponse]): Unit = {
        respondTo(responseObserver) {
            val ws = manager.getWorkspace(request.getWorkspaceName)
            GetWorkspaceResponse.newBuilder()
                .setWorkspace(
                    WorkspaceDetails.newBuilder()
                        .setName(ws.name)
                        .addAllProjects(ws.listProjects().map(_.name).asJava)
                        .build()
                )
                .build()
        }
    }

    /**
     */
    override def deleteWorkspace(request: DeleteWorkspaceRequest, responseObserver: StreamObserver[DeleteWorkspaceResponse]): Unit = {
        respondTo(responseObserver) {
            manager.deleteWorkspace(request.getWorkspaceName)
            DeleteWorkspaceResponse.getDefaultInstance
        }
    }

    /**
     */
    override def cleanWorkspace(request: CleanWorkspaceRequest, responseObserver: StreamObserver[CleanWorkspaceResponse]): Unit = {
        respondTo(responseObserver) {
            val ws = manager.getWorkspace(request.getWorkspaceName);
            ws.clean()
            CleanWorkspaceResponse.newBuilder().build()
        }
    }

    /**
     */
    override def uploadFiles(responseObserver: StreamObserver[UploadFilesResponse]): StreamObserver[UploadFilesRequest] = {
        var ws: com.dimajix.flowman.storage.Workspace = null
        var wsName: String = null
        streamRequests(responseObserver) { req:UploadFilesRequest =>
            if (wsName == null) {
                wsName = req.getWorkspaceName
                ws = manager.getWorkspace(wsName)
            } else {
                if (wsName != req.getWorkspaceName) {
                    throw new IllegalArgumentException("All file upload requests must belong to the same workspace")
                }
                println(s"Receive file ${req.getFileName}")
            }
        }{
            UploadFilesResponse.newBuilder().build()
        }
    }
}
