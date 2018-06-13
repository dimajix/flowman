/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.server.master

import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.Duration

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.slf4j.LoggerFactory

import com.dimajix.flowman.server.master.api.ActionService
import com.dimajix.flowman.server.master.api.MappingService
import com.dimajix.flowman.server.master.api.ModelService
import com.dimajix.flowman.server.master.api.NamespaceService
import com.dimajix.flowman.server.master.api.ProjectService
import com.dimajix.flowman.server.master.api.SwaggerDocService


class Server(args:Arguments) {
    private val logger = LoggerFactory.getLogger(classOf[Server])

    def run() : Boolean = {
        import akka.http.scaladsl.server.Directives._

        implicit val system = ActorSystem("flowman")
        implicit val materializer = ActorMaterializer()
        // needed for the future flatMap/onComplete in the end
        implicit val executionContext = system.dispatcher

        logger.info("Setting up routes")

        // Create all services
        val namespace = new NamespaceService
        val project = new ProjectService
        val mapping = new MappingService
        val action = new ActionService
        val model = new ModelService

        // Create routes from services
        val route =
            pathPrefix("api" / "v1") {
                namespace.route() ~
                project.route() ~
                mapping.route() ~
                action.route() ~
                model.route() ~
                SwaggerDocService.routes
            } ~
            pathPrefix("swagger") {
                path("") {
                    getFromResource("swagger/index.html")
                } ~
                getFromResourceDirectory("META-INF/resources/webjars/swagger-ui/3.13.0")
            }

        logger.info("Starting http server")
        Http().bind(args.bindHost, args.bindPort).to(Sink.foreach { connection =>
            logger.info("Accepted new connection from " + connection.remoteAddress)
            connection.handleWith(route)
        }).run()

        logger.info(s"Server online at http://${args.bindHost}:${args.bindPort}/")
        Await.ready(Promise[Done].future, Duration.Inf)
        true
    }

}
