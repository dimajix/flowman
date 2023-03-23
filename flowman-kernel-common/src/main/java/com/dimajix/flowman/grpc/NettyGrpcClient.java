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

import java.net.URI;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import static com.dimajix.flowman.kernel.ThreadPoolExecutor.newExecutor;


public class NettyGrpcClient extends GrpcClient {
    private final ManagedChannel channel;

    public NettyGrpcClient(URI uri) {
        this.channel = createChannel(uri);
    }

    @Override
    public ManagedChannel getChannel() {
        return channel;
    }

    private static ManagedChannel createChannel(URI uri) {
        ManagedChannelBuilder<?> channelBuilder;
        try {
            channelBuilder = NettyGrpcClientImpl.createChannel(uri);
        }
        catch (ClassNotFoundException|NoClassDefFoundError ex) {
            try {
                channelBuilder = ShadedNettyGrpcClientImpl.createChannel(uri);
            }
            catch (ClassNotFoundException|NoClassDefFoundError ex2) {
                throw new RuntimeException("No Netty binding found for gRPC", ex2);
            }
        }

        return channelBuilder
            .executor(newExecutor())
            .maxInboundMetadataSize(1024*1024)
            .usePlaintext()
            .build();
    }
}


final class NettyGrpcClientImpl {
    private NettyGrpcClientImpl() {
    }

    static ManagedChannelBuilder<?> createChannel(URI uri) throws ClassNotFoundException, NoClassDefFoundError {
        return io.grpc.netty.NettyChannelBuilder
            .forAddress(uri.getHost(), uri.getPort());
    }
}


final class ShadedNettyGrpcClientImpl {
    private ShadedNettyGrpcClientImpl() {
    }

    static ManagedChannelBuilder<?> createChannel(URI uri)  throws ClassNotFoundException, NoClassDefFoundError {
        return io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
            .forAddress(uri.getHost(), uri.getPort());
    }
}
