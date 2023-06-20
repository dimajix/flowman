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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.LinkedList;

import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerTransportFilter;
import lombok.val;

import static com.dimajix.flowman.kernel.ThreadPoolExecutor.newExecutor;


public class GrpcServerBuilder {
    private final Builder builder;
    private final LinkedList<GrpcService> services = new LinkedList<>();
    private final LinkedList<ServerTransportFilter> transportFilters = new LinkedList<>();
    private final LinkedList<ServerInterceptor> interceptors = new LinkedList<>();

    private interface Builder {
        ServerBuilder<?> build();
    }

    static public GrpcServerBuilder forAddress(SocketAddress address) {
        Builder builder = () -> NettyServerBuilder.forAddress(address);
        return new GrpcServerBuilder(builder);
    }
    static public GrpcServerBuilder forName(String name) {
        Builder builder = () -> InProcessServerBuilder.forName(name);
        return new GrpcServerBuilder(builder);
    }

    private GrpcServerBuilder(Builder builder) {
        this.builder = builder;
    }

    public GrpcServerBuilder withServices(Iterable<GrpcService> services) {
        for (val service : services)
            this.services.add(service);
        return this;
    }

    public GrpcServerBuilder withTransportFilters( Iterable<ServerTransportFilter> transportFilters) {
        for (val filter : transportFilters)
            this.transportFilters.add(filter);
        return this;
    }
    public GrpcServerBuilder withTransportFilter( ServerTransportFilter transportFilter) {
        this.transportFilters.add(transportFilter);
        return this;
    }

    public GrpcServerBuilder withInterceptors( Iterable<ServerInterceptor> interceptors) {
        for (val interceptor : interceptors)
            this.interceptors.add(interceptor);
        return this;
    }
    public GrpcServerBuilder withInterceptor( ServerInterceptor interceptor) {
        this.interceptors.add(interceptor);
        return this;
    }

    public GrpcServer build() {
        val serverBuilder = builder.build();

        serverBuilder
            .maxInboundMetadataSize(1024*1024)
            .executor(newExecutor());

        for (val service : services)
            serverBuilder.addService(service);

        for (val interceptor : interceptors)
            serverBuilder.intercept(interceptor);

        for (val filter : transportFilters)
            serverBuilder.addTransportFilter(filter);

        val server = serverBuilder.build();
        return new GrpcServer(server);
    }
}
