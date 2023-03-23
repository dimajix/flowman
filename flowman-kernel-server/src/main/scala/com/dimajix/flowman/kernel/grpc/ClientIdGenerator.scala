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

import io.grpc.Attributes
import io.grpc.ServerTransportFilter

import com.dimajix.flowman.kernel.grpc.ClientIdGenerator.CLIENT_ID_KEY;


object ClientIdGenerator {
    val CLIENT_ID_KEY: Attributes.Key[UUID] = Attributes.Key.create("client-id")
}
class ClientIdGenerator(watchers:ClientWatcher*) extends ServerTransportFilter {
    override def transportReady(transportAttrs: Attributes) : Attributes = {
        val clientId = UUID.randomUUID()
        val attributes = transportAttrs.toBuilder
            .set(CLIENT_ID_KEY, clientId)
            .build()
        watchers.foreach(_.clientDisconnected(clientId))
        attributes
    }

    override def transportTerminated(transportAttrs: Attributes): Unit = {
        val clientId = transportAttrs.get(CLIENT_ID_KEY)
        if (clientId != null) {
            watchers.foreach(_.clientDisconnected(clientId))
        }
    }
}
