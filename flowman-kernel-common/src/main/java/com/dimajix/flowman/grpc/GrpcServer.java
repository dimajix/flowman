/*
 * Copyright 2018-2023 Kaya Kupferschmidt
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

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;

import io.grpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


abstract public class GrpcServer {
    private static final Logger logger = LoggerFactory.getLogger(GrpcServer.class.getName());
    private final Server server;


    private final Thread shutdownHook = new Thread() {
        @Override
        public void run() {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            logger.info("Shutting down gRPC server since JVM is shutting down");
            GrpcServer.this.stop();
            logger.info("gRPC server has been shut down");
        }
    };

    public GrpcServer(Server server) {
        this.server = server;
    }

    public void start() throws IOException {
        logger.info("Starting gRPC server");
        server.start();
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    public void stop() {
        if (server != null) {
            logger.info("Stopping gRPC server");
            server.shutdown();
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            }
            catch(IllegalStateException ex) {
            }
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void awaitTermination() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            }
            catch(IllegalStateException ex) {
            }
        }
    }

    public boolean isTerminated() {
        return server.isTerminated();
    }

    public Server getServer() { return server; }

    public List<? extends SocketAddress> getListenSockets() {
        return server.getListenSockets();
    }
}
