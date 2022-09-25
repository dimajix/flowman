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

package com.dimajix.flowman.server.rest

import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Promise

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.Found
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import org.slf4j.LoggerFactory

import com.dimajix.common.net.SocketUtils
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.history.JobQuery


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

        val namespaceService = new NamespaceService(session.namespace.get)
        val jobHistoryService = new JobHistoryService(session.history)
        val targetHistoryService = new TargetHistoryService(session.history)
        val metricService = new MetricService(session.history)

        val apiRoute = pathPrefix("api") (
                SwaggerDocService.routes
                ~
                namespaceService.routes
                ~
                pathPrefix("history") {
                    (
                        jobHistoryService.routes
                            ~
                            targetHistoryService.routes
                            ~
                            metricService.routes
                        )
                }
            )
        val swaggerUiRoute = pathPrefix("swagger") (
                pathEndOrSingleSlash {
                    redirectToTrailingSlashIfMissing(Found) {
                        get {
                            getFromResource("swagger/index.html")
                        }
                    }
                }
                ~
                get {
                    getFromResourceDirectory("META-INF/resources/webjars/swagger-ui/4.1.3")
                }
            )
        val flowmanUiRoute = (
                pathEndOrSingleSlash {
                    get {
                        getFromResource("META-INF/resources/webjars/flowman-server-ui/index.html")
                    }
                }
                ~
                get {
                    getFromResourceDirectory("META-INF/resources/webjars/flowman-server-ui")
                }
            )
        val route = extractRequestContext { ctx =>
            extractClientIP { ip =>
                logger.info(s"Client ${ip} ${ctx.request.method.value} ${ctx.request.uri.path}")
                apiRoute ~
                swaggerUiRoute ~
                flowmanUiRoute
            }
        }

        logger.info("Connecting to history backend")
        session.history.countJobs(JobQuery())

        logger.info("Starting http server")

        val settings = ServerSettings(system)
            .withVerboseErrorMessages(true)
            .withRemoteAddressHeader(true)

        val (binding,server) = Http().bind(conf.getBindHost(), conf.getBindPort(), akka.http.scaladsl.ConnectionContext.noEncryption(), settings)
            .toMat(Sink.foreach { connection =>
                connection.handleWith(
                    route
                )
            })(Keep.both)
            .run()

        binding.foreach { binding =>
            val listenUrl = SocketUtils.toURL("http", binding.localAddress, allowAny = true)
            logger.info(s"Flowman Server online at $listenUrl")
        }

        Await.ready(server, Duration.Inf)
    }
}
