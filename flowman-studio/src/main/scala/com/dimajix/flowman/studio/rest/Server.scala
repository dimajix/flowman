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

package com.dimajix.flowman.studio.rest

import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Promise
import scala.concurrent.duration.Duration

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes.Found
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.slf4j.LoggerFactory

import com.dimajix.common.net.SocketUtils
import com.dimajix.flowman.studio.Configuration
import com.dimajix.flowman.studio.service.KernelManager
import com.dimajix.flowman.studio.service.LauncherManager
import com.dimajix.flowman.studio.service.LocalLauncher


class Server(
    conf:Configuration
) {
    import akka.http.scaladsl.server.Directives._

    private val logger = LoggerFactory.getLogger(classOf[Server])

    implicit private val system: ActorSystem = ActorSystem("flowman")
    implicit private val materializer: ActorMaterializer = ActorMaterializer()
    implicit private val executionContext: ExecutionContextExecutor = system.dispatcher

    private val launcherManager = new LauncherManager
    private val kernelManager = new KernelManager
    private val pingEndpoint = new PingEndpoint
    private val launcherEndpoint = new LauncherEndpoint(launcherManager)
    private val registryEndpoint = new RegistryEndpoint(kernelManager)
    private val kernelEndpoint = new KernelEndpoint(kernelManager, launcherManager)

    def run(): Unit = {
        val route = (
                pathPrefix("api") {(
                    pingEndpoint.routes
                    ~
                    registryEndpoint.routes
                    ~
                    launcherEndpoint.routes
                    ~
                    kernelEndpoint.routes
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

        logger.info("Starting Flowman Studio")

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
            logger.info(s"Flowman Studio online at $listenUrl")

            val localUrl = SocketUtils.toURL("http", binding.localAddress, allowAny = false)

            launcherManager.addLauncher(new LocalLauncher(localUrl, system))
        }

        Await.ready(Promise[Done].future, Duration.Inf)
    }
}
