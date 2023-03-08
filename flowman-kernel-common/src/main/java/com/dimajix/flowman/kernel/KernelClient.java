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

package com.dimajix.flowman.kernel;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.grpc.ManagedChannel;
import lombok.val;

import com.dimajix.flowman.kernel.model.Kernel;
import com.dimajix.flowman.kernel.model.Namespace;
import com.dimajix.flowman.kernel.proto.kernel.GetKernelRequest;
import com.dimajix.flowman.kernel.proto.kernel.GetNamespaceRequest;
import com.dimajix.flowman.kernel.proto.kernel.KernelServiceGrpc;
import com.dimajix.flowman.kernel.proto.session.CreateSessionRequest;
import com.dimajix.flowman.kernel.proto.session.GetSessionRequest;
import com.dimajix.flowman.kernel.proto.session.ListSessionsRequest;
import com.dimajix.flowman.kernel.proto.session.SessionServiceGrpc;
import com.dimajix.flowman.kernel.proto.workspace.CreateWorkspaceRequest;
import com.dimajix.flowman.kernel.proto.workspace.DeleteWorkspaceRequest;
import com.dimajix.flowman.kernel.proto.workspace.GetWorkspaceRequest;
import com.dimajix.flowman.kernel.proto.workspace.ListWorkspacesRequest;
import com.dimajix.flowman.kernel.proto.workspace.WorkspaceServiceGrpc;


public final class KernelClient extends AbstractClient {
    private final ManagedChannel channel;
    private final KernelServiceGrpc.KernelServiceBlockingStub kernelStub;
    private final WorkspaceServiceGrpc.WorkspaceServiceBlockingStub workspaceStub;
    private final SessionServiceGrpc.SessionServiceBlockingStub sessionStub;

    public KernelClient(ManagedChannel channel) {
        this.channel = channel;
        kernelStub = KernelServiceGrpc.newBlockingStub(channel);
        workspaceStub = WorkspaceServiceGrpc.newBlockingStub(channel);
        sessionStub = SessionServiceGrpc.newBlockingStub(channel);
    }

    public List<SessionClient> listSessions() {
        val request = ListSessionsRequest.newBuilder().build();
        val result = call(() -> sessionStub.listSessions(request));
        val l = result.getSessionsList();
        return l.stream().map(s -> new SessionClient(channel, s.getId())).collect(Collectors.toList());
    }

    public SessionClient createSession(String workspace, String projectLocation, Map<String,String> config, Map<String,String> environment, List<String> profiles) {
        val request = CreateSessionRequest.newBuilder()
            .setWorkspace(workspace)
            .setProjectLocation(projectLocation)
            .setName(projectLocation)
            .addAllProfiles(profiles)
            .putAllConfig(config)
            .putAllEnvironment(environment)
            .build();

        val result = call(() -> sessionStub.createSession(request));
        return getSession(result.getSession().getId());
    }
    public SessionClient createSession(String projectLocation, Map<String,String> config, Map<String,String> environment, List<String> profiles) {
        val request = CreateSessionRequest.newBuilder()
            .setProjectLocation(projectLocation)
            .addAllProfiles(profiles)
            .putAllConfig(config)
            .putAllEnvironment(environment)
            .build();

        val result = call(() -> sessionStub.createSession(request));
        return getSession(result.getSession().getId());
    }

    public SessionClient getSession(String sessionId) {
        val request = GetSessionRequest.newBuilder().setSessionId(sessionId).build();
        val result = call(() -> sessionStub.getSession(request));
        return new SessionClient(channel, result.getSession().getId());
    }

    public List<WorkspaceClient> listWorkspaces() {
        val request = ListWorkspacesRequest.newBuilder().build();
        val result = call(() -> workspaceStub.listWorkspaces(request));
        val l = result.getWorkspacesList();
        return l.stream().map(w -> new WorkspaceClient(channel, w.getName())).collect(Collectors.toList());

    }

    public WorkspaceClient getWorkspace(String workspaceName) {
        val request = GetWorkspaceRequest.newBuilder()
            .setWorkspaceName(workspaceName)
            .build();
        val result = call(() -> workspaceStub.getWorkspace(request));
        val ws = result.getWorkspace();
        return new WorkspaceClient(channel, ws.getName());
    }

    public WorkspaceClient createWorkspace(String workspaceName, boolean ifNotExists) {
        val request = CreateWorkspaceRequest.newBuilder()
            .setName(workspaceName)
            .setIfNotExists(ifNotExists)
            .build();
        val result = call(() -> workspaceStub.createWorkspace(request));
        return getWorkspace(result.getWorkspace().getName());
    }

    public void deleteWorkspace(String workspaceName) {
        val request = DeleteWorkspaceRequest.newBuilder()
            .setWorkspaceName(workspaceName)
            .build();
        call(() -> workspaceStub.deleteWorkspace(request));
    }

    public Kernel getKernel() {
        val request = GetKernelRequest.newBuilder()
            .build();
        val result = call(() -> kernelStub.getKernel(request));
        val kernel = result.getKernel();
        return Kernel.ofProto(kernel);
    }

    public Namespace getNamespace() {
        val request = GetNamespaceRequest.newBuilder()
            .build();
        val result = call(() -> kernelStub.getNamespace(request));
        val namespace = result.getNamespace();
        return Namespace.ofProto(namespace);
    }
}
