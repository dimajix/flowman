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

import io.grpc.ServerBuilder;


public class NettyServerBuilder {

    public static ServerBuilder<?> forPort(int port) {
        try {
            return ShadedNettyServerImpl.createBuilder(port);
        }
        catch (ClassNotFoundException|NoClassDefFoundError ex) {
            try {
                return NettyServerImpl.createBuilder(port);
            }
            catch (ClassNotFoundException|NoClassDefFoundError ex2) {
                throw new RuntimeException("No Netty binding found for gRPC", ex2);
            }
        }
    }
}


final class NettyServerImpl {
    private NettyServerImpl() {
    }

    static ServerBuilder<?> createBuilder(int port) throws ClassNotFoundException, NoClassDefFoundError {
        return io.grpc.netty.NettyServerBuilder
            .forPort(port)
            .permitKeepAliveWithoutCalls(true);
    }
}


final class ShadedNettyServerImpl {
    private ShadedNettyServerImpl() {
    }

    static ServerBuilder<?> createBuilder(int port) throws ClassNotFoundException, NoClassDefFoundError {
        return io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
            .forPort(port)
            .permitKeepAliveWithoutCalls(true);
    }
}
