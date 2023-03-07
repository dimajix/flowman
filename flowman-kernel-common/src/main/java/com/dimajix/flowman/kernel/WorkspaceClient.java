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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.StreamSupport;

import com.google.common.io.Files;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dimajix.flowman.kernel.proto.FileType;
import com.dimajix.flowman.kernel.proto.workspace.CleanWorkspaceRequest;
import com.dimajix.flowman.kernel.proto.workspace.UploadFilesRequest;
import com.dimajix.flowman.kernel.proto.workspace.UploadFilesResponse;
import com.dimajix.flowman.kernel.proto.workspace.WorkspaceServiceGrpc;


public final class WorkspaceClient extends AbstractClient {
    private final Logger logger = LoggerFactory.getLogger(WorkspaceClient.class);
    private final ManagedChannel channel;
    private final WorkspaceServiceGrpc.WorkspaceServiceBlockingStub blockingStub;
    private final WorkspaceServiceGrpc.WorkspaceServiceStub asyncStub;
    private final String workspaceName;


    public WorkspaceClient(ManagedChannel channel, String workspaceName) {
        this.channel = channel;
        this.workspaceName = workspaceName;
        blockingStub = WorkspaceServiceGrpc.newBlockingStub(channel);
        asyncStub = WorkspaceServiceGrpc.newStub(channel);
    }

    public String getWorkspaceName() { return workspaceName; }

    public void cleanWorkspace() {
        val request = CleanWorkspaceRequest.newBuilder()
            .setWorkspaceName(workspaceName)
            .build();
        call(() -> blockingStub.cleanWorkspace(request));
    }

    public void uploadWorkspace(File localDirectory) throws IOException {
        if (!localDirectory.exists())
            throw new IllegalArgumentException("Local workspace directory '" + localDirectory + "' does not exist");
        if (!localDirectory.isDirectory())
            throw new IllegalArgumentException("Local workspace '" + localDirectory + "' must be a directory, but it is not");

        val root = localDirectory.getAbsoluteFile().getCanonicalFile();
        val rootPath = root.toPath();
        logger.info("Uploading local workspace directory '" + rootPath + "' to server...");

        val files = Files.fileTraverser().breadthFirst(root);
        val iter = StreamSupport.stream(files.spliterator(), false)
            .filter(file -> !file.equals(root))
            .map(file -> uploadFile(rootPath, file))
            .iterator();
        stream(asyncStub::uploadFiles, iter);
    }
    private UploadFilesRequest uploadFile(Path root, File src) {
        val filename = root.relativize(src.toPath()).toString();
        logger.info("Uploading file '" + src + "' as '" + filename + "' to server...");
        val result = UploadFilesRequest.newBuilder()
                .setWorkspaceName(workspaceName)
                .setFileName(filename);
        if (src.isDirectory()) {
            result.setFileType(FileType.DIRECTORY);
        }
        else {
            try {
                val content = Files.toByteArray(src);
                result.setFileType(FileType.FILE);
                result.setFileContent(ByteString.copyFrom(content));
            }
            catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        return result.build();
    }
}
