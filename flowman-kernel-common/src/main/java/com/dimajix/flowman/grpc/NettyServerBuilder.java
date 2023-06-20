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

import java.net.SocketAddress;

import io.grpc.ServerBuilder;


public class NettyServerBuilder {

    public static ServerBuilder<?> forAddress(SocketAddress address) {
        try {
            return ShadedNettyServerImpl.createBuilder(address);
        }
        catch (ClassNotFoundException|NoClassDefFoundError ex) {
            try {
                return NettyServerImpl.createBuilder(address);
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

    static ServerBuilder<?> createBuilder(SocketAddress address) throws ClassNotFoundException, NoClassDefFoundError {
        return io.grpc.netty.NettyServerBuilder
            .forAddress(address)
            .permitKeepAliveWithoutCalls(true);
    }
}


final class ShadedNettyServerImpl {
    private ShadedNettyServerImpl() {
    }

    static ServerBuilder<?> createBuilder(SocketAddress address) throws ClassNotFoundException, NoClassDefFoundError {
        return io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
            .forAddress(address)
            .permitKeepAliveWithoutCalls(true);
    }
}
