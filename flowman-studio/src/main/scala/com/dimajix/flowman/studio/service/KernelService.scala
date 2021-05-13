/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.studio.service

import java.net.URL

import scala.concurrent.Future

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse


sealed abstract class KernelState
object KernelState {
    case object STARTING extends KernelState
    case object RUNNING extends KernelState
    case object STOPPING extends KernelState
    case object TERMINATED extends KernelState
}

final class KernelService(val id:String, val secret: String, process: Process)(implicit system:ActorSystem) {
    private var _url:Option[URL] = None

    def url : Option[URL] = _url
    private[service] def setUrl(url:URL) : Unit = {_url = Some(url)}

    /**
     * Returns the state of the kernel as known by the service. This may polling the process state and its
     * registration state
     * @return
     */
    def state : KernelState = {
        process.state match {
            case ProcessState.STARTING => KernelState.STARTING
            case ProcessState.RUNNING =>
                if (url.isEmpty)
                    KernelState.STARTING
                else
                    KernelState.RUNNING
            case ProcessState.STOPPING => KernelState.STOPPING
            case ProcessState.TERMINATED => KernelState.TERMINATED
        }
    }

    /**
     * Returns true if the Kernel is still alive
     * @return
     */
    def isAlive() : Boolean = {
        process.state == ProcessState.RUNNING
    }

    /**
     * Stops the Kernel
     */
    def shutdown() : Unit = {
        process.shutdown()
    }

    def invoke(request:HttpRequest) : Future[HttpResponse] = {
        _url match {
            case Some(url) =>
                val uri = request.uri.withHost(url.getHost).withPort(url.getPort)
                val finalRequest = request.copy(uri=uri)
                Http().singleRequest(finalRequest)
        }
    }
}
