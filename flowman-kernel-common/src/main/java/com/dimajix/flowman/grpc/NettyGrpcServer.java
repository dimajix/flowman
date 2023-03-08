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

package com.dimajix.flowman.grpc;

import io.grpc.BindableService;
import io.grpc.Server;
import lombok.val;

import static com.dimajix.flowman.kernel.ThreadPoolExecutor.newExecutor;


public class NettyGrpcServer extends GrpcServer {
    public NettyGrpcServer(int port, Iterable<GrpcService> services) {
        super(createServer(port, services));
    }

    private static Server createServer(int port, Iterable<GrpcService> services) {
        try {
            return ShadedNettyGrpcServerImpl.createServer(port, services);
        }
        catch (ClassNotFoundException|NoClassDefFoundError ex) {
            try {
                return NettyGrpcServerImpl.createServer(port, services);
            }
            catch (ClassNotFoundException|NoClassDefFoundError ex2) {
                throw new RuntimeException("No Netty binding found for gRPC", ex2);
            }
        }
    }
}


final class NettyGrpcServerImpl {
    private NettyGrpcServerImpl() {
    }

    static Server createServer(int port, Iterable<GrpcService> services) throws ClassNotFoundException, NoClassDefFoundError {
        val builder = io.grpc.netty.NettyServerBuilder
            .forPort(port)
            .permitKeepAliveWithoutCalls(true)
            .maxInboundMetadataSize(1024*1024)
            .executor(newExecutor());

        for (BindableService service : services)
            builder.addService(service);

        return builder.build();
    }
}


final class ShadedNettyGrpcServerImpl {
    private ShadedNettyGrpcServerImpl() {
    }

    static Server createServer(int port, Iterable<GrpcService> services) throws ClassNotFoundException, NoClassDefFoundError {
        val builder = io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
            .forPort(port)
            .permitKeepAliveWithoutCalls(true)
            .maxInboundMetadataSize(1024*1024)
            .executor(newExecutor());

        for (BindableService service : services)
            builder.addService(service);

        return builder.build();
    }
}
