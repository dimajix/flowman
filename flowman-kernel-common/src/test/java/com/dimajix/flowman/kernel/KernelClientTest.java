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

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import lombok.val;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dimajix.flowman.grpc.GrpcServer;
import com.dimajix.flowman.grpc.GrpcServerBuilder;
import com.dimajix.flowman.grpc.RemoteException;


public class KernelClientTest {
    private GrpcServer inprocessServer;
    private ManagedChannel channel;
    private KernelClient client;


    public KernelClientTest() {
        super();
    }

    @Test
    public void testGetWorkspace() throws InterruptedException {
        try {
            val ws = client.getWorkspace("no_such_workspace");
        } finally {
            shutdown();
        }
    }

    @Test
    public void testUnimplementedException() throws InterruptedException {
        try {
            val ex = assertThrows(StatusRuntimeException.class, () -> client.createWorkspace("no_such_workspace", true));
            assertEquals(Status.UNIMPLEMENTED.getCode(), ex.getStatus().getCode());
        } finally {
            shutdown();
        }
    }

    @Test
    public void testUserException() throws InterruptedException {
        try {
            val re = assertThrows(RemoteException.class, () -> client.listWorkspaces());
            assertEquals("This is not supported.", re.getMessage());
            assertEquals("java.lang.IllegalArgumentException", ((RemoteException)re).getDeclaredClass());
        } finally {
            shutdown();
        }
    }

    @BeforeEach
    public void beforeEachTest() throws IOException {
        inprocessServer = GrpcServerBuilder.forName("test")
            .withServices(Collections.singletonList(new WorkspaceDummyImpl()))
            .build();
        inprocessServer.start();
        channel = InProcessChannelBuilder
            .forName("test")
            .directExecutor()
            .usePlaintext()
            .build();
        client = new KernelClient(channel);
    }

    @AfterEach
    public void afterEachTest(){
        if (channel != null) {
            channel.shutdownNow();
            channel = null;
        }
        if (inprocessServer != null) {
            inprocessServer.stop();
            inprocessServer = null;
        }
    }

    public void shutdown() throws InterruptedException {
        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            channel = null;
        }
    }
}
