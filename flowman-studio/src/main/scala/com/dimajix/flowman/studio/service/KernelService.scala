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

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Post
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.Uri
import org.reactivestreams.Publisher


sealed abstract class KernelState
object KernelState {
    case object STARTING extends KernelState
    case object RUNNING extends KernelState
    case object STOPPING extends KernelState
    case object TERMINATED extends KernelState
}

final class KernelService(val id:String, val secret: String, process: Process)(implicit system:ActorSystem) {
    private implicit val ec: ExecutionContextExecutor = system.dispatcher
    private var _url:Option[URL] = None

    def url : Option[URL] = _url
    private[service] def setUrl(url:URL) : Unit = {_url = Some(url)}

    /**
     * Returns a publisher for all console messages produced by the process
     * @return
     */
    def messages : Publisher[String] = process.messages

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
     * Stops the Kernel by issuing a shutdown request to the kernel via its REST interface.
     */
    def shutdown() : Unit = {
        _url match {
            case Some(url) =>
                val uri = Uri(url.toString).withPath(Uri.Path("/api/shutdown"))
                Http().singleRequest(Post(uri))
                    .onComplete {
                        case Success(res) =>
                            println(res)
                        case Failure(_) =>
                            process.shutdown()
                    }
            case None =>
                process.shutdown()
        }
    }

    /**
     * Forwards a [[HttpRequest]] to the kernel. This function will replace the host and port by the appropriate
     * values, but the path has already to be correct. Note that the resulting [[HttpResponse]] will not be modified,
     * which means that a HTTP redirect will contain the kernel host for example.
     * @param request
     * @return
     */
    def invoke(request:HttpRequest) : Future[HttpResponse] = {
        _url match {
            case Some(url) =>
                val uri = request.uri.withHost(url.getHost).withPort(url.getPort)
                val finalRequest = request.copy(uri=uri)
                Http().singleRequest(finalRequest)
            case None => Future.successful(HttpResponse(StatusCodes.BadGateway))
        }
    }
}
