/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package com.dimajix.flowman.grpc;

import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server side interceptor that is used to put remote client's IP Address to thread local storage.
 */
public class LogCallServerInjector implements ServerInterceptor {
    private final Logger logger = LoggerFactory.getLogger(LogCallServerInjector.class);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                 Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        val remoteIpAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString();
        val method = call.getMethodDescriptor().getFullMethodName();
        logger.info(remoteIpAddress + " - " + method);
        return next.startCall(call, headers);
    }
}
