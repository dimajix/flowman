/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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
import com.dimajix.flowman.studio.Configuration
import com.dimajix.flowman.studio.model.StudioRegistrationRequest
import com.dimajix.flowman.studio.rest.session.SessionEndpoint
import com.dimajix.flowman.studio.rest.workspace.WorkspaceEndpoint
import com.dimajix.flowman.studio.service.SessionManager
import com.dimajix.flowman.studio.service.WorkspaceManager


class Server(
    conf:Configuration,
    rootSession:Session
) {
    import akka.http.scaladsl.client.RequestBuilding._
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.studio.model.JsonSupport._

    private val logger = LoggerFactory.getLogger(classOf[Server])

    implicit private  val system: ActorSystem = ActorSystem("flowman")
    implicit private val materializer: ActorMaterializer = ActorMaterializer()
    implicit private val executionContext: ExecutionContextExecutor = system.dispatcher

    private val workspaceRoot = {
        val root = conf.getWorkspaceRoot()
        val fs = rootSession.fs

        // Load Project. If no schema is specified, load from local file system
        val uri = root.toUri
        if (uri.getAuthority == null && uri.getScheme == null)
            fs.local(root).absolute
        else
            fs.file(root).absolute
    }
    private val sessionManager = new SessionManager(rootSession)
    private val workspaceManager = new WorkspaceManager(workspaceRoot)

    private val shutdownPromise = Promise[Done]()
    private val shutdownEndpoint = new ShutdownEndpoint(shutdownPromise.trySuccess(Done))
    private val pingEndpoint = new PingEndpoint
    private val workspaceEndpoint = new WorkspaceEndpoint(workspaceManager)
    private val namespaceEndpoint = new NamespaceEndpoint(rootSession.namespace.get)
    private val sessionEndpoint = new SessionEndpoint(workspaceManager, sessionManager)

    def run(): Unit = {
        val route = (
                pathPrefix("api") {(
                    shutdownEndpoint.routes
                    ~
                    pingEndpoint.routes
                    ~
                    workspaceEndpoint.routes
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
                    getFromResourceDirectory("META-INF/resources/webjars/swagger-ui/4.1.3")
                )}
                ~
                pathEndOrSingleSlash {
                    getFromResource("META-INF/resources/webjars/flowman-studio-ui/index.html")
                }
                ~
                getFromResourceDirectory("META-INF/resources/webjars/flowman-studio-ui")
            )

        logger.info("Starting Flowman Studio")

        val settings = ServerSettings(system)
            .withVerboseErrorMessages(true)
            .withRemoteAddressHeader(true)

        val server = Http().bind(conf.getBindHost(), conf.getBindPort(), akka.http.scaladsl.ConnectionContext.noEncryption(), settings)
            .to(Sink.foreach { connection =>
                connection.handleWith(
                    extractRequestContext { ctx =>
                        extractClientIP { ip =>
                            logger.info(s"Client ${ip} ${ctx.request.method.value} ${ctx.request.uri.path}")
                            route
                        }
                    }
                )
            })
            .run()

        server.foreach { binding =>
            val listenUrl = SocketUtils.toURL("http", binding.localAddress, allowAny = true)
            logger.info(s"Flowman Studio online at $listenUrl")

            register(binding.localAddress)
        }

        Await.ready(shutdownPromise.future, Duration.Inf)
    }

    /**
     * Register Studio at Flowman Hub
     * @param localAddress
     */
    private def register(localAddress:InetSocketAddress) : Unit = {
        conf.getHubUrl().foreach { url =>
            val localUrl = SocketUtils.toURL("http", localAddress)

            logger.info(s"Registering Flowman Studio running at $localUrl with Flowman Hub running at $url")
            val request = StudioRegistrationRequest(id=conf.getStudioId(), url=localUrl.toString)
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
