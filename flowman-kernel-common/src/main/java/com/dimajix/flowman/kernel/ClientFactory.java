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

package com.dimajix.flowman.kernel;

import java.net.URI;

import io.grpc.ManagedChannel;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dimajix.flowman.grpc.InProcessGrpcClient;
import com.dimajix.flowman.grpc.NettyGrpcClient;


public class ClientFactory {
    private static final Logger logger = LoggerFactory.getLogger(ClientFactory.class);

    public static KernelClient createClient(URI uri) {
        logger.info("Connecting to Flowman kernel at " + uri.toString());

        ManagedChannel channel;
        if (uri.getScheme().equals("inprocess")) {
           channel = createInprocessChannel(uri);
        }
        else if (uri.getScheme().equals("http") || uri.getScheme().equals("grpc")) {
            channel = createHttpChannel(uri);
        }
        else {
            throw new IllegalArgumentException("Network scheme '" + uri.getScheme() + "' not supported for connecting to kernel");
        }

        return new KernelClient(channel);
    }

    private static ManagedChannel createInprocessChannel(URI uri) {
        val client = new InProcessGrpcClient(uri);
        return client.getChannel();
    }

    private static ManagedChannel createHttpChannel(URI uri) {
        val client = new NettyGrpcClient(uri);
        return client.getChannel();
    }
}
