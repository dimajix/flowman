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

package com.dimajix.flowman.grpc;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import static com.dimajix.flowman.kernel.ThreadPoolExecutor.newExecutor;


public class NettyGrpcServer extends GrpcServer {
    public NettyGrpcServer(int port, Iterable<GrpcService> services) {
        super(createServer(port, services));
    }

    private static Server createServer(int port, Iterable<GrpcService> services) {
        ServerBuilder<?> serverBuilder;
        try {
            serverBuilder = ShadedNettyGrpcServerImpl.createServer(port);
        }
        catch (ClassNotFoundException|NoClassDefFoundError ex) {
            try {
                serverBuilder = NettyGrpcServerImpl.createServer(port);
            }
            catch (ClassNotFoundException|NoClassDefFoundError ex2) {
                throw new RuntimeException("No Netty binding found for gRPC", ex2);
            }
        }

        serverBuilder
            .permitKeepAliveWithoutCalls(true)
            .intercept(new LogCallServerInjector())
            .maxInboundMetadataSize(1024*1024)
            .executor(newExecutor());

        for (BindableService service : services)
            serverBuilder.addService(service);

        return serverBuilder.build();
    }
}


final class NettyGrpcServerImpl {
    private NettyGrpcServerImpl() {
    }

    static ServerBuilder<?> createServer(int port) throws ClassNotFoundException, NoClassDefFoundError {
        return io.grpc.netty.NettyServerBuilder
            .forPort(port);
    }
}


final class ShadedNettyGrpcServerImpl {
    private ShadedNettyGrpcServerImpl() {
    }

    static ServerBuilder<?> createServer(int port) throws ClassNotFoundException, NoClassDefFoundError {
        return io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
            .forPort(port);
    }
}
