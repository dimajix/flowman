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

package com.dimajix.flowman.kernel.rest

import akka.http.scaladsl.server
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.Path

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.kernel.model.Converter
import com.dimajix.flowman.kernel.model.Namespace


@Api(value = "/session", produces = "application/json", consumes = "application/json")
@Path("/session")
class SessionEndpoint(rootSession:Session) {
    import akka.http.scaladsl.server.Directives._
    import com.dimajix.flowman.kernel.model.JsonSupport._

    private val projectService:ProjectEndpoint = new ProjectEndpoint
    private val jobService:JobEndpoint = new JobEndpoint
    private val mappingService:MappingEndpoint = new MappingEndpoint
    private val relationService:RelationEndpoint = new RelationEndpoint
    private val targetService:TargetEndpoint = new TargetEndpoint
    private val testService:TestEndpoint = new TestEndpoint

    def routes : server.Route = pathPrefix("session") {(
        pathEndOrSingleSlash {(
            list()
            ~
            create()
        )}
        ~
        pathPrefix(Segment) { session => (
            pathEndOrSingleSlash {
                close(session)
            }
            ~
            projectService.routes
            ~
            jobService.routes
            ~
            mappingService.routes
            ~
            relationService.routes
            ~
            targetService.routes
            ~
            testService.routes
        )}
    )}

    @ApiOperation(value = "Return list of all active sessions", nickname = "getSessions", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Information about namespace", response = classOf[Namespace])
    ))
    def list() : server.Route = {
        get {
            complete(Converter.ofSpec(ns))
        }
    }

    @ApiOperation(value = "Create new session", nickname = "createSession", httpMethod = "POST")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Information about namespace", response = classOf[Namespace])
    ))
    def create() : server.Route = {
        post {
            complete(Converter.ofSpec(ns))
        }
    }

    @ApiOperation(value = "Close session", nickname = "closeSession", httpMethod = "DELETE")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Information about namespace", response = classOf[Namespace])
    ))
    def close(session:String) : server.Route = {
        delete {
            complete(Converter.ofSpec(ns))
        }
    }
}
