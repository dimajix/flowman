/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.kernel.rest

import java.net.InetSocketAddress

import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.Found
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.slf4j.LoggerFactory

import com.dimajix.common.net.SocketUtils
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.kernel.Configuration
import com.dimajix.flowman.kernel.model.KernelRegistrationRequest


class Server(
    conf:Configuration,
    rootSession:Session
) {
    import akka.http.scaladsl.client.RequestBuilding._
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.kernel.model.JsonSupport._

    private val logger = LoggerFactory.getLogger(classOf[Server])

    implicit private  val system: ActorSystem = ActorSystem("flowman")
    implicit private val materializer: ActorMaterializer = ActorMaterializer()
    implicit private val executionContext: ExecutionContextExecutor = system.dispatcher

    private val shutdownPromise = Promise[Done]()
    private val shutdownEndpoint = new ShutdownEndpoint(shutdownPromise.trySuccess(Done))
    private val pingEndpoint = new PingEndpoint
    private val projectEndpoint = new ProjectEndpoint(rootSession.store)
    private val namespaceEndpoint = new NamespaceEndpoint(rootSession.namespace.get)
    private val sessionEndpoint = new SessionEndpoint(rootSession)

    def run(): Unit = {
        val route = (
                pathPrefix("api") {(
                    shutdownEndpoint.routes
                    ~
                    pingEndpoint.routes
                    ~
                    projectEndpoint.routes
                    ~
                    namespaceEndpoint.routes
                    ~
                    sessionEndpoint.routes
                    ~
                    SwaggerDocEndpoint.routes
                )}
                ~
                pathPrefix("swagger") {(
                    pathEndOrSingleSlash {
                        redirectToTrailingSlashIfMissing(Found) {
                            getFromResource("swagger/index.html")
                        }
                    }
                    ~
                    getFromResourceDirectory("META-INF/resources/webjars/swagger-ui/3.22.2")
                )}
            )

        logger.info("Starting Flowman kernel")

        val settings = ServerSettings(system)
            .withVerboseErrorMessages(true)

        val server = Http().bind(conf.getBindHost(), conf.getBindPort(), akka.http.scaladsl.ConnectionContext.noEncryption(), settings)
            .to(Sink.foreach { connection =>
                logger.info("Accepted new connection from " + connection.remoteAddress)
                connection.handleWith(route)
            })
            .run()

        server.foreach { binding =>
            val listenUrl = SocketUtils.toURL("http", binding.localAddress, allowAny = true)
            logger.info(s"Flowman kernel online at $listenUrl")

            register(binding.localAddress)
        }

        Await.ready(shutdownPromise.future, Duration.Inf)
    }

    /**
     * Register kernel at Flowman Studio
     * @param localAddress
     */
    private def register(localAddress:InetSocketAddress) : Unit = {
        conf.getStudioUrl().foreach { url =>
            val localUrl = SocketUtils.toURL("http", localAddress)

            logger.info(s"Registering Flowman kernel running at $localUrl with Flowman Studio running at $url")
            val request = KernelRegistrationRequest(id=conf.getKernelId(), url=localUrl.toString)
            val studioUri = Uri(url.toString)
            val uri = studioUri.withPath(studioUri.path / "api" / "registry")

            Http().singleRequest(Post(uri, request))
                .onComplete {
                    case Success(res) => println(res)
                    case Failure(_) => sys.error("something wrong")
                }
        }
    }
}
