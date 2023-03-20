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

import io.grpc.stub.StreamObserver
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.kernel.grpc.ClientIdExtractor.CLIENT_ID
import com.dimajix.flowman.kernel.proto.FileType
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


final class WorkspaceServiceHandler(
    workspaceManager:WorkspaceManager
) extends WorkspaceServiceGrpc.WorkspaceServiceImplBase with ServiceHandler with ClientWatcher{
    override protected final val logger = LoggerFactory.getLogger(classOf[WorkspaceServiceHandler])

    override def clientDisconnected(clientId: UUID): Unit = {
        workspaceManager.removeClientWorkspaces(clientId)
    }

    /**
     */
    override def createWorkspace(request: CreateWorkspaceRequest, responseObserver: StreamObserver[CreateWorkspaceResponse]): Unit = {
        respondTo("createWorkspace", responseObserver) {
            val hasId = request.hasId && request.getId.nonEmpty
            val clientId = if (hasId) None else Some(CLIENT_ID.get())
            val id = if (request.hasId) request.getId else UUID.randomUUID().toString
            val (wid,ws) = {
                if (hasId && request.getIfNotExists && workspaceManager.list().exists(_._1 == id)) {
                    workspaceManager.getWorkspace(id)
                }
                else {
                    workspaceManager.createWorkspace(id, request.getName, clientId)
                }
            };
            CreateWorkspaceResponse.newBuilder()
                .setWorkspace(WorkspaceDetails.newBuilder()
                    .setId(wid)
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
        respondTo("listWorkspaces", responseObserver) {
            val list = workspaceManager.list()
            val ws = list.map { case(id,ws) => Workspace.newBuilder().setId(id).setName(ws.name).build() }
            ListWorkspacesResponse.newBuilder()
                .addAllWorkspaces(ws.asJava)
                .build()
        }
    }

    /**
     */
    override def getWorkspace(request: GetWorkspaceRequest, responseObserver: StreamObserver[GetWorkspaceResponse]): Unit = {
        respondTo("getWorkspace", responseObserver) {
            val id = request.getWorkspaceId
            val (wid,ws) = workspaceManager.getWorkspace(id)
            GetWorkspaceResponse.newBuilder()
                .setWorkspace(
                    WorkspaceDetails.newBuilder()
                        .setId(wid)
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
        respondTo("deleteWorkspace", responseObserver) {
            workspaceManager.deleteWorkspace(request.getWorkspaceId)
            DeleteWorkspaceResponse.getDefaultInstance
        }
    }

    /**
     */
    override def cleanWorkspace(request: CleanWorkspaceRequest, responseObserver: StreamObserver[CleanWorkspaceResponse]): Unit = {
        respondTo("cleanWorkspace", responseObserver) {
            val (_,ws) = workspaceManager.getWorkspace(request.getWorkspaceId);
            ws.clean()
            CleanWorkspaceResponse.newBuilder().build()
        }
    }

    /**
     */
    override def uploadFiles(responseObserver: StreamObserver[UploadFilesResponse]): StreamObserver[UploadFilesRequest] = {
        var ws: com.dimajix.flowman.storage.Workspace = null
        var wsName: String = null
        streamRequests("uploadFiles", responseObserver) { req:UploadFilesRequest =>
            if (wsName == null) {
                wsName = req.getWorkspaceId
                logger.info(s"Receiving file updates for workspace '$wsName'")
                val (_,ws0) = workspaceManager.getWorkspace(wsName)
                ws = ws0
            } else {
                if (wsName != req.getWorkspaceId) {
                    throw new IllegalArgumentException("All file upload requests must belong to the same workspace")
                }
            }
            val parcel = ws.parcels.head
            req.getFileType match {
                case FileType.FILE if req.hasFileContent =>
                    logger.debug(s"Workspace '$wsName' receives file '${req.getFileName}'")
                    parcel.putFile(new Path(req.getFileName), req.getFileContent.toByteArray)
                case FileType.DIRECTORY =>
                    logger.info(s"Workspace '$wsName' receives directory '${req.getFileName}'")
                    parcel.mkdir(new Path(req.getFileName))
                case _ =>
                    throw new IllegalArgumentException(s"File type '${req.getFileType}' not supported")
            }
        }{
            logger.info(s"Successfully finished file updates for workspace '$wsName'")
            UploadFilesResponse.newBuilder().build()
        }
    }
}
