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

package com.dimajix.flowman.kernel.grpc;

import java.util.UUID

import io.grpc.ForwardingServerCallListener
import io.grpc.Grpc
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status
import org.slf4j.LoggerFactory

import com.dimajix.flowman.kernel.grpc.ClientIdExtractor.CLIENT_ID;


object ClientIdExtractor {
    val CLIENT_ID:ThreadLocal[UUID] = new ThreadLocal[UUID]()
}
class ClientIdExtractor extends ServerInterceptor {
    private val logger = LoggerFactory.getLogger(classOf[ClientIdExtractor])

    override def interceptCall[ReqT,RespT](call:ServerCall[ReqT, RespT], headers:Metadata, next:ServerCallHandler[ReqT, RespT]) : ServerCall.Listener[ReqT] = {
        // Log Method call
        val attributes = call.getAttributes()
        val clientId = attributes.get(ClientIdGenerator.CLIENT_ID_KEY)
        val remoteIpAddress = attributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString()
        val method = call.getMethodDescriptor().getFullMethodName()
        logger.info(s"[$clientId]$remoteIpAddress - $method")

        /**
         * For streaming calls, below will make sure client id is injected prior to creating
         * the stream. If the call gets closed during authentication, the listener we return below
         * will not continue.
         */
        extractClientId(call, headers);

        /**
         * For non-streaming calls to server, below listener will be invoked in the same thread that is
         * serving the call.
         */
        new ForwardingServerCallListener.SimpleForwardingServerCallListener[ReqT](next.startCall(call, headers)) {
            override def onHalfClose() : Unit = {
                if (extractClientId(call, headers))
                    delegate().onHalfClose()
            }
        };
    }

    private def extractClientId[ReqT,RespT](call:ServerCall[ReqT,RespT], headers:Metadata) : Boolean = {
        val attributes = call.getAttributes()
        val clientId = attributes.get(ClientIdGenerator.CLIENT_ID_KEY)
        if (clientId == null) {
            closeQuietly(call, Status.UNAUTHENTICATED.withDescription("No client id"), headers)
            return false;
        }
        CLIENT_ID.set(clientId);
        true
    }

    /**
     * Closes the call while blanketing runtime exceptions. This is mostly to avoid dumping "already
     * closed" exceptions to logs.
     *
     * @param call call to close
     * @param status status to close the call with
     * @param headers headers to close the call with
     */
    private def closeQuietly[ReqT,RespT](call:ServerCall[ReqT,RespT], status:Status, headers:Metadata) : Unit = {
        try {
            logger.debug(s"Closing the call:${call.getMethodDescriptor().getFullMethodName()} with Status:$status")
            call.close(status, headers);
        }
        catch {
            case exc:RuntimeException =>
                logger.debug(s"Error while closing the call:${call.getMethodDescriptor().getFullMethodName()} with Status:$status", exc)
        }
    }
}
