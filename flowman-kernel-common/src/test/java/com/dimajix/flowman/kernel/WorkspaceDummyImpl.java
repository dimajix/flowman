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

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.val;

import com.dimajix.flowman.grpc.ExceptionUtils;
import com.dimajix.flowman.grpc.GrpcService;
import com.dimajix.flowman.kernel.proto.workspace.*;


public class WorkspaceDummyImpl extends WorkspaceServiceGrpc.WorkspaceServiceImplBase implements GrpcService {
    @Override
    public void createWorkspace(CreateWorkspaceRequest request, StreamObserver<CreateWorkspaceResponse> responseObserver) {
        super.createWorkspace(request, responseObserver);
    }

    @Override
    public void listWorkspaces(ListWorkspacesRequest request, StreamObserver<ListWorkspacesResponse> responseObserver) {
        val rawEx = new IllegalArgumentException("This is not supported.");
        val ex = ExceptionUtils.asStatusException(Status.INTERNAL, rawEx, true);
        responseObserver.onError(ex);
    }

    @Override
    public void getWorkspace(GetWorkspaceRequest request, StreamObserver<GetWorkspaceResponse> responseObserver) {
        val result = GetWorkspaceResponse.newBuilder()
                .build();
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteWorkspace(DeleteWorkspaceRequest request, StreamObserver<DeleteWorkspaceResponse> responseObserver) {
        super.deleteWorkspace(request, responseObserver);
    }
}
