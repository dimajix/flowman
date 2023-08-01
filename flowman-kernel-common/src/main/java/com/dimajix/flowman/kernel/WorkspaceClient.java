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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import com.dimajix.flowman.kernel.proto.workspace.GetWorkspaceRequest;
import com.google.common.io.Files;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dimajix.flowman.kernel.proto.FileType;
import com.dimajix.flowman.kernel.proto.workspace.CleanWorkspaceRequest;
import com.dimajix.flowman.kernel.proto.workspace.UploadFilesRequest;
import com.dimajix.flowman.kernel.proto.workspace.WorkspaceServiceGrpc;
import com.dimajix.common.io.Globber;


public final class WorkspaceClient extends AbstractClient {
    private final Logger logger = LoggerFactory.getLogger(WorkspaceClient.class);
    private final ManagedChannel channel;
    private final WorkspaceServiceGrpc.WorkspaceServiceBlockingStub blockingStub;
    private final WorkspaceServiceGrpc.WorkspaceServiceStub asyncStub;
    private final String workspaceId;


    public WorkspaceClient(ManagedChannel channel, String workspaceId) {
        this.channel = channel;
        this.workspaceId = workspaceId;
        blockingStub = WorkspaceServiceGrpc.newBlockingStub(channel);
        asyncStub = WorkspaceServiceGrpc.newStub(channel);
    }

    @Override
    public boolean isShutdown() {
        return channel.isTerminated();
    }

    @Override
    public boolean isTerminated() {
        return channel.isTerminated();
    }

    public List<String> listProjects() {
        val request = GetWorkspaceRequest.newBuilder()
            .setWorkspaceId(workspaceId)
            .build();
        val result = call(() -> blockingStub.getWorkspace(request));
        return result.getWorkspace().getProjectsList();
    }

    public String getWorkspaceId() { return workspaceId; }

    public void cleanWorkspace() {
        val request = CleanWorkspaceRequest.newBuilder()
            .setWorkspaceId(workspaceId)
            .build();
        call(() -> blockingStub.cleanWorkspace(request));
    }

    public void uploadWorkspace(File localDirectory) throws IOException {
        if (!localDirectory.exists())
            throw new IllegalArgumentException("Local workspace directory '" + localDirectory + "' does not exist");
        if (!localDirectory.isDirectory())
            throw new IllegalArgumentException("Local workspace '" + localDirectory + "' must be a directory, but it is not");

        val globber = new Globber(localDirectory);
        val rootPath = globber.getRootPath();
        logger.info("Uploading local directory '" + rootPath + "' to workspace '" + workspaceId + "'...");

        val maxMessageSize = 4194304;
        val iter = globber.glob()
            .map(file -> uploadFile(rootPath, file, maxMessageSize))
            .iterator();
        stream(asyncStub::uploadFiles, iter);
    }

    private UploadFilesRequest uploadFile(Path root, Path src, int maxMessageSize) {
        val filename = root.relativize(src).toString();
        val result = UploadFilesRequest.newBuilder()
                .setWorkspaceId(workspaceId)
                .setFileName(filename);

        val msgSize = result.build().toByteArray().length;
        val srcFile = src.toFile();
        if (srcFile.isDirectory()) {
            logger.info("Uploading directory'" + src + "' as '" + filename + "' to kernel...");
            result.setFileType(FileType.DIRECTORY);
        }
        else if (srcFile.isFile()) {
            logger.debug("Uploading file '" + src + "' as '" + filename + "' to kernel...");
            try {
                val fileSize = srcFile.length();
                if (fileSize + msgSize + 1024 > maxMessageSize) {
                    val msg = "File '" + src + "' is too big to transfer to Flowman kernel workspace, " + fileSize + " bytes. You can exclude files in .flowman-ignore";
                    logger.error(msg);
                    throw new IllegalArgumentException(msg);
                }
                val content = Files.toByteArray(srcFile);
                result.setFileType(FileType.FILE);
                result.setFileContent(ByteString.copyFrom(content));
            }
            catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        else {
            logger.info("Ignoring special file'" + src + "'...");
        }
        return result.build();
    }
}
