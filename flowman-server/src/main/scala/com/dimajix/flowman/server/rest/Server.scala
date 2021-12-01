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
import akka.http.scaladsl.server.Directives.pathPrefix
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.slf4j.LoggerFactory

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

        val route = (
                pathPrefix("api") {(
                    SwaggerDocService.routes
                    ~
                    namespaceService.routes
                    ~
                    pathPrefix("history") {(
                        jobHistoryService.routes
                        ~
                        targetHistoryService.routes
                        ~
                        metricService.routes
                    )}
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
                ~
                pathEndOrSingleSlash {
                    getFromResource("META-INF/resources/webjars/flowman-server-ui/index.html")
                }
                ~
                getFromResourceDirectory("META-INF/resources/webjars/flowman-server-ui")
            )

        logger.info("Connecting to history backend")
        session.history.countJobs(JobQuery())

        logger.info("Starting http server")

        val settings = ServerSettings(system)
            .withVerboseErrorMessages(true)

        Http().bind(conf.getBindHost(), conf.getBindPort(), akka.http.scaladsl.ConnectionContext.noEncryption(), settings)
            .to(Sink.foreach { connection =>
                logger.info("Accepted new connection from " + connection.remoteAddress)
                connection.handleWith(route)
            })
            .run()

        logger.info(s"Server online at http://${conf.getBindHost()}:${conf.getBindPort()}/")
        Await.ready(Promise[Done].future, Duration.Inf)
    }
}
