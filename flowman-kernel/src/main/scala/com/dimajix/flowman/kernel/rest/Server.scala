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

import java.net.InetAddress

import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.StatusCodes.Found
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.kernel.Configuration


class Server(
    conf:Configuration,
    session:Session
) {
    import akka.http.scaladsl.server.Directives._

    private val logger = LoggerFactory.getLogger(classOf[Server])

    implicit val system: ActorSystem = ActorSystem("flowman")

    def run(): Unit = {
        import scala.concurrent.duration._
        implicit val materializer: ActorMaterializer = ActorMaterializer()
        implicit val executionContext: ExecutionContextExecutor = system.dispatcher

        val pingEndpoint = new PingEndpoint
        val namespaceEndpoint = new NamespaceEndpoint(session.namespace.get)
        val sessionEndpoint = new SessionEndpoint(session)

        val route = (
                pathPrefix("api") {(
                    pingEndpoint.routes
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
            logger.info(s"Flowman kernel online at http://[${binding.localAddress.getAddress.getHostAddress}]:${binding.localAddress.getPort}")

            // Register at Flowman Studio
            conf.getStudioUrl().foreach { url =>
                val localhost = InetAddress.getLocalHost
                val localIpAddress = localhost.getHostAddress
                val localPort = binding.localAddress.getPort

                logger.info(s"Registering Kernel at [$localIpAddress]:$localPort with Flowman Studio running at $url")
                val studioUri = Uri(url.toString)
                val uri = studioUri.withPath(studioUri.path / "api" / "registry")
                val responseFuture = Http().singleRequest(HttpRequest(uri = uri, method = HttpMethods.POST))

                responseFuture
                    .onComplete {
                        case Success(res) => println(res)
                        case Failure(_) => sys.error("something wrong")
                    }
            }
        }

        Await.ready(Promise[Done].future, Duration.Inf)
    }
}
